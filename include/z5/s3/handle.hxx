#pragma once
#include <atomic>
#include <cstdlib>
#include <map>
#include <memory>
#include <mutex>
#include <tuple>
// aws includes
#include <aws/core/Aws.h>
#include <aws/core/utils/logging/ConsoleLogSystem.h>
#include <aws/core/auth/AWSCredentials.h>
#include <aws/core/auth/AWSCredentialsProvider.h>
#include <aws/core/auth/AWSCredentialsProviderChain.h>
#include <aws/core/utils/memory/AWSMemory.h>
#include <aws/core/utils/memory/stl/AWSStringStream.h>
#include <aws/core/client/AWSError.h>
#include <aws/core/http/HttpResponse.h>
#include <aws/s3/S3Client.h>
#include <aws/s3/S3ClientConfiguration.h>
#include <aws/s3/S3Errors.h>
#include <aws/s3/S3EndpointProvider.h>
#include <aws/s3/model/Object.h>
#include <aws/s3/model/ObjectIdentifier.h>
#include <aws/s3/model/Delete.h>
#include <aws/s3/model/ListObjectsV2Request.h>
#include <aws/s3/model/GetObjectRequest.h>
#include <aws/s3/model/PutObjectRequest.h>
#include <aws/s3/model/HeadObjectRequest.h>
#include <aws/s3/model/DeleteObjectRequest.h>
#include <aws/s3/model/DeleteObjectsRequest.h>

#include "z5/handle.hxx"


namespace z5 {
namespace s3 {

namespace detail {

    // Initialize the AWS C++ SDK exactly once per process (and shut it down at
    // process exit). The previous implementation called Aws::InitAPI /
    // Aws::ShutdownAPI around every single operation, which is both slow and
    // unsafe under concurrency. A function-local static gives us thread-safe,
    // one-shot initialization.
    struct AwsApiGuard {
        Aws::SDKOptions options;
        AwsApiGuard() {
            if(std::getenv("Z5_S3_DEBUG")) {
                options.loggingOptions.logLevel = Aws::Utils::Logging::LogLevel::Debug;
                options.loggingOptions.logger_create_fn = [](){
                    return Aws::MakeShared<Aws::Utils::Logging::ConsoleLogSystem>(
                        "z5", Aws::Utils::Logging::LogLevel::Debug);
                };
            }
            Aws::InitAPI(options);
        }
        ~AwsApiGuard() { Aws::ShutdownAPI(options); }
    };

    inline void ensureApiInitialized() {
        static AwsApiGuard guard;
    }

    // small allocation tag for Aws::MakeShared
    static constexpr const char * ALLOC_TAG = "z5";

    // join an object-key prefix with a name (no leading slash for a root prefix)
    inline std::string joinKey(const std::string & base, const std::string & name) {
        return base.empty() ? name : base + "/" + name;
    }

    // build an S3 client configured for the given endpoint / region / credentials.
    // A custom endpoint (MinIO / moto / ...) forces path-style addressing, since
    // virtual-host addressing ("bucket.host") does not resolve against such servers.
    inline Aws::S3::S3Client makeConfiguredClient(const std::string & endpoint,
                                                  const std::string & region,
                                                  const bool anon,
                                                  const std::string & accessKey,
                                                  const std::string & secretKey) {
        ensureApiInitialized();
        // Disable the EC2 instance-metadata service probe: the default
        // S3ClientConfiguration constructor otherwise tries to resolve the region
        // from IMDS (169.254.169.254), which hangs for a long time on any non-EC2
        // host (incl. CI / dev machines / containers).
        Aws::Client::ClientConfigurationInitValues initValues;
        initValues.shouldDisableIMDS = true;
        Aws::S3::S3ClientConfiguration config(initValues);
        // finite timeouts so a misconfigured endpoint fails fast instead of hanging
        config.connectTimeoutMs = 5000;
        config.requestTimeoutMs = 60000;
        if(!region.empty()) {
            config.region = Aws::String(region.c_str(), region.size());
        }
        if(!endpoint.empty()) {
            // aws-sdk-cpp expects endpointOverride as a bare "host:port"; the scheme
            // is configured separately. Passing a "http(s)://" prefix here makes the
            // SDK build a malformed URL and stall, so strip it off.
            std::string host = endpoint;
            if(host.rfind("http://", 0) == 0) {
                config.scheme = Aws::Http::Scheme::HTTP;
                host = host.substr(7);
            } else if(host.rfind("https://", 0) == 0) {
                config.scheme = Aws::Http::Scheme::HTTPS;
                host = host.substr(8);
            }
            config.endpointOverride = Aws::String(host.c_str(), host.size());
            // custom endpoints (MinIO / moto / ...) require path-style addressing;
            // virtual-host style ("bucket.host") does not resolve there.
            config.useVirtualAddressing = false;
        }

        std::shared_ptr<Aws::Auth::AWSCredentialsProvider> credentials;
        if(anon) {
            credentials = Aws::MakeShared<Aws::Auth::AnonymousAWSCredentialsProvider>(ALLOC_TAG);
        } else if(!accessKey.empty()) {
            credentials = Aws::MakeShared<Aws::Auth::SimpleAWSCredentialsProvider>(
                ALLOC_TAG, Aws::String(accessKey.c_str(), accessKey.size()),
                Aws::String(secretKey.c_str(), secretKey.size()));
        } else {
            credentials = Aws::MakeShared<Aws::Auth::DefaultAWSCredentialsProviderChain>(ALLOC_TAG);
        }
        auto endpointProvider = Aws::MakeShared<Aws::S3::S3EndpointProvider>(ALLOC_TAG);
        return Aws::S3::S3Client(credentials, endpointProvider, config);
    }

    // Return the process-wide shared client for a given configuration, creating it
    // on first use. Aws::S3::S3Client is thread-safe and intended to be shared;
    // constructing one per operation pays for credential-chain resolution, endpoint
    // setup and a fresh HTTP connection pool every time.
    inline std::shared_ptr<Aws::S3::S3Client> getSharedClient(const std::string & endpoint,
                                                              const std::string & region,
                                                              const bool anon,
                                                              const std::string & accessKey,
                                                              const std::string & secretKey) {
        // the API guard must be fully constructed before the cache below, so that the
        // cached clients are destroyed BEFORE Aws::ShutdownAPI runs at process exit
        ensureApiInitialized();
        typedef std::tuple<std::string, std::string, bool, std::string, std::string> ClientKey;
        static std::mutex cacheMutex;
        static std::map<ClientKey, std::shared_ptr<Aws::S3::S3Client>> cache;

        const ClientKey key{endpoint, region, anon, accessKey, secretKey};
        std::lock_guard<std::mutex> lock(cacheMutex);
        auto it = cache.find(key);
        if(it == cache.end()) {
            it = cache.emplace(key, std::make_shared<Aws::S3::S3Client>(
                makeConfiguredClient(endpoint, region, anon, accessKey, secretKey))).first;
        }
        return it->second;
    }

    // get the shared client for any z5 handle (reads the config via the base-Handle API)
    inline std::shared_ptr<Aws::S3::S3Client> makeClient(const z5::handle::Handle & handle) {
        return getSharedClient(handle.endpoint(), handle.region(), handle.anon(),
                               handle.accessKey(), handle.secretKey());
    }

    //
    // low-level object IO helpers (used by handle / dataset / metadata / attributes)
    //

    // true if the error means "no such object"; anything else (network, auth,
    // timeout, ...) must NOT be treated as absence
    inline bool isNotFound(const Aws::Client::AWSError<Aws::S3::S3Errors> & error) {
        return error.GetErrorType() == Aws::S3::S3Errors::NO_SUCH_KEY ||
               error.GetErrorType() == Aws::S3::S3Errors::RESOURCE_NOT_FOUND ||
               error.GetResponseCode() == Aws::Http::HttpResponseCode::NOT_FOUND;
    }

    inline std::runtime_error makeS3Error(const std::string & what, const std::string & key,
                                          const Aws::Client::AWSError<Aws::S3::S3Errors> & error) {
        return std::runtime_error("z5: " + what + ": " + key + " (" +
                                  std::string(error.GetMessage().c_str()) + ")");
    }

    // read an object's bytes; returns false if the object does not exist,
    // throws on any other error
    inline bool getObject(Aws::S3::S3Client & client,
                          const std::string & bucket, const std::string & key,
                          std::vector<char> & out) {
        Aws::S3::Model::GetObjectRequest request;
        request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
        request.SetKey(Aws::String(key.c_str(), key.size()));
        auto outcome = client.GetObject(request);
        if(!outcome.IsSuccess()) {
            if(isNotFound(outcome.GetError())) {
                return false;
            }
            throw makeS3Error("could not read object from S3", key, outcome.GetError());
        }
        auto result = outcome.GetResultWithOwnership();
        const long long length = result.GetContentLength();
        auto & body = result.GetBody();
        if(length >= 0) {
            out.resize(static_cast<std::size_t>(length));
            if(length > 0) {
                body.read(out.data(), length);
                if(body.gcount() != static_cast<std::streamsize>(length)) {
                    throw std::runtime_error("z5: truncated response body from S3: " + key);
                }
            }
        } else {
            // unknown content length: read the stream to EOF (binary-safe)
            out.clear();
            char chunk[65536];
            do {
                body.read(chunk, sizeof(chunk));
                out.insert(out.end(), chunk, chunk + body.gcount());
            } while(body.gcount() > 0);
        }
        return true;
    }

    // read an object as a string; returns false if the object does not exist
    inline bool getObjectString(Aws::S3::S3Client & client,
                               const std::string & bucket, const std::string & key,
                               std::string & out) {
        std::vector<char> bytes;
        if(!getObject(client, bucket, key, bytes)) {
            return false;
        }
        out.assign(bytes.begin(), bytes.end());
        return true;
    }

    // write an object from raw bytes (binary-safe)
    inline void putObject(Aws::S3::S3Client & client,
                         const std::string & bucket, const std::string & key,
                         const char * data, const std::size_t size) {
        Aws::S3::Model::PutObjectRequest request;
        request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
        request.SetKey(Aws::String(key.c_str(), key.size()));
        auto body = Aws::MakeShared<Aws::StringStream>(ALLOC_TAG);
        body->write(data, size);
        request.SetBody(body);
        auto outcome = client.PutObject(request);
        if(!outcome.IsSuccess()) {
            throw std::runtime_error("z5: could not write object to S3: " + key +
                                     " (" + std::string(outcome.GetError().GetMessage().c_str()) + ")");
        }
    }

    inline void putObjectString(Aws::S3::S3Client & client,
                               const std::string & bucket, const std::string & key,
                               const std::string & content) {
        putObject(client, bucket, key, content.data(), content.size());
    }

    inline bool objectExists(Aws::S3::S3Client & client,
                            const std::string & bucket, const std::string & key) {
        Aws::S3::Model::HeadObjectRequest request;
        request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
        request.SetKey(Aws::String(key.c_str(), key.size()));
        return client.HeadObject(request).IsSuccess();
    }

    inline void deleteObject(Aws::S3::S3Client & client,
                           const std::string & bucket, const std::string & key) {
        Aws::S3::Model::DeleteObjectRequest request;
        request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
        request.SetKey(Aws::String(key.c_str(), key.size()));
        auto outcome = client.DeleteObject(request);
        // deleting a non-existing object is fine (DeleteObject is idempotent)
        if(!outcome.IsSuccess() && !isNotFound(outcome.GetError())) {
            throw makeS3Error("could not delete object from S3", key, outcome.GetError());
        }
    }

    // collect all object keys under a prefix (paginated)
    inline void listAllKeys(Aws::S3::S3Client & client,
                          const std::string & bucket, const std::string & prefix,
                          std::vector<std::string> & out) {
        Aws::S3::Model::ListObjectsV2Request request;
        request.WithBucket(Aws::String(bucket.c_str(), bucket.size()));
        request.WithPrefix(Aws::String(prefix.c_str(), prefix.size()));
        Aws::S3::Model::ListObjectsV2Result result;
        do {
            auto outcome = client.ListObjectsV2(request);
            if(!outcome.IsSuccess()) {
                throw makeS3Error("could not list objects in S3", prefix, outcome.GetError());
            }
            result = outcome.GetResult();
            for(const auto & object : result.GetContents()) {
                const auto & k = object.GetKey();
                out.emplace_back(k.c_str(), k.size());
            }
            request.SetContinuationToken(result.GetNextContinuationToken());
        } while(result.GetIsTruncated());
    }

    // delete every object under a prefix (batched, up to 1000 keys per request)
    inline void deletePrefix(Aws::S3::S3Client & client,
                           const std::string & bucket, const std::string & prefix) {
        std::vector<std::string> keys;
        listAllKeys(client, bucket, prefix, keys);
        const std::size_t batchSize = 1000;
        for(std::size_t start = 0; start < keys.size(); start += batchSize) {
            Aws::S3::Model::Delete del;
            const std::size_t end = std::min(start + batchSize, keys.size());
            for(std::size_t i = start; i < end; ++i) {
                Aws::S3::Model::ObjectIdentifier oid;
                oid.SetKey(Aws::String(keys[i].c_str(), keys[i].size()));
                del.AddObjects(oid);
            }
            Aws::S3::Model::DeleteObjectsRequest request;
            request.SetBucket(Aws::String(bucket.c_str(), bucket.size()));
            request.SetDelete(del);
            auto outcome = client.DeleteObjects(request);
            if(!outcome.IsSuccess()) {
                throw makeS3Error("could not delete objects from S3", prefix, outcome.GetError());
            }
        }
    }

}  // namespace detail


namespace handle {

    // common functionality for S3 File and Group handles. Stores the bucket /
    // key as well as the client configuration (endpoint / region / credentials)
    // so that any handle can build a correctly-configured S3 client.
    class S3HandleImpl {
    public:
        S3HandleImpl(const std::string & bucketName, const std::string & nameInBucket,
                     const std::string & endpoint="", const std::string & region="us-east-1",
                     const bool anon=false, const std::string & accessKey="",
                     const std::string & secretKey="")
            : bucketName_(bucketName),
              nameInBucket_(nameInBucket),
              endpoint_(endpoint),
              region_(region),
              anon_(anon),
              accessKey_(accessKey),
              secretKey_(secretKey) {
            detail::ensureApiInitialized();
        }
        ~S3HandleImpl() {}

        // the (process-wide shared) client for this handle's endpoint / region / credentials
        inline std::shared_ptr<Aws::S3::S3Client> makeClient() const {
            return detail::getSharedClient(endpoint_, region_, anon_, accessKey_, secretKey_);
        }

        // list with maxKeys=1 to check whether any object exists under a prefix.
        // The prefix must include the trailing "/" delimiter where appropriate,
        // otherwise sibling names that merely share a prefix match too
        // (e.g. "data" would match "data2/...").
        inline bool anyObjectWithPrefix(Aws::S3::S3Client & client,
                                        const std::string & prefix) const {
            Aws::S3::Model::ListObjectsV2Request request;
            request.WithBucket(Aws::String(bucketName_.c_str(), bucketName_.size()));
            request.WithPrefix(Aws::String(prefix.c_str(), prefix.size()));
            request.WithMaxKeys(1);
            const auto outcome = client.ListObjectsV2(request);
            if(!outcome.IsSuccess()) {
                throw detail::makeS3Error("could not list objects in S3", prefix, outcome.GetError());
            }
            return outcome.GetResult().GetKeyCount() > 0;
        }

        // check if this handle exists (any object under the prefix)
        inline bool existsImpl() const {
            auto client = makeClient();
            const std::string prefix = nameInBucket_.empty() ? "" : nameInBucket_ + "/";
            return anyObjectWithPrefix(*client, prefix);
        }

        inline void keysImpl(std::vector<std::string> & out) const {
            auto client = makeClient();
            Aws::S3::Model::ListObjectsV2Request request;
            request.WithBucket(Aws::String(bucketName_.c_str(), bucketName_.size()));
            // add delimiter to the prefix
            const std::string bucketPrefix = nameInBucket_ == "" ? "" : nameInBucket_ + "/";
            request.WithPrefix(Aws::String(bucketPrefix.c_str(), bucketPrefix.size()));
            request.WithDelimiter("/");

            Aws::S3::Model::ListObjectsV2Result object_list;
            do {
                auto outcome = client->ListObjectsV2(request);
                if(!outcome.IsSuccess()) {
                    break;
                }
                object_list = outcome.GetResult();
                for(const auto & common_prefix : object_list.GetCommonPrefixes()) {
                    const std::string prefix(common_prefix.GetPrefix().c_str(),
                                             common_prefix.GetPrefix().size());
                    if(!prefix.empty() && prefix.back() == '/') {
                        std::vector<std::string> prefixSplit;
                        util::split(prefix, prefixSplit, "/");
                        out.emplace_back(prefixSplit.rbegin()[1]);
                    }
                }
                request.SetContinuationToken(object_list.GetNextContinuationToken());
            } while(object_list.GetIsTruncated());
        }

        inline bool inImpl(const std::string & name) const {
            auto client = makeClient();
            const std::string key = detail::joinKey(nameInBucket_, name);
            // exact object (metadata file or zarr v2 "."-delimited chunk)
            if(detail::objectExists(*client, bucketName_, key)) {
                return true;
            }
            // sub-group / sub-dataset: anything under "name/"
            return anyObjectWithPrefix(*client, key + "/");
        }

        // remove every object under this handle's prefix
        inline void removeImpl() const {
            auto client = makeClient();
            const std::string prefix = nameInBucket_ == "" ? "" : nameInBucket_ + "/";
            detail::deletePrefix(*client, bucketName_, prefix);
        }

        inline bool isZarrGroup() const {
            // zarr v2 uses .zgroup, zarr v3 uses zarr.json
            return inImpl(".zgroup") || inImpl("zarr.json");
        }
        inline bool isZarrDataset() const {
            // zarr v2 uses .zarray, zarr v3 uses zarr.json
            return inImpl(".zarray") || inImpl("zarr.json");
        }

        inline const std::string & bucketNameImpl() const {return bucketName_;}
        inline const std::string & nameInBucketImpl() const {return nameInBucket_;}
        inline const std::string & endpointImpl() const {return endpoint_;}
        inline const std::string & regionImpl() const {return region_;}
        inline bool anonImpl() const {return anon_;}
        inline const std::string & accessKeyImpl() const {return accessKey_;}
        inline const std::string & secretKeyImpl() const {return secretKey_;}

    private:
        std::string bucketName_;
        std::string nameInBucket_;
        std::string endpoint_;
        std::string region_;
        bool anon_;
        std::string accessKey_;
        std::string secretKey_;
    };


    // forward the stored client configuration through the z5::handle::Handle API
    #define Z5_S3_CONFIG_API \
        inline const std::string & bucketName() const {return bucketNameImpl();} \
        inline const std::string & nameInBucket() const {return nameInBucketImpl();} \
        inline const std::string & endpoint() const {return endpointImpl();} \
        inline const std::string & region() const {return regionImpl();} \
        inline bool anon() const {return anonImpl();} \
        inline const std::string & accessKey() const {return accessKeyImpl();} \
        inline const std::string & secretKey() const {return secretKeyImpl();}


    class File : public z5::handle::File<File>, private S3HandleImpl {
    public:
        typedef z5::handle::File<File> BaseType;

        File(const std::string & bucketName,
             const std::string & nameInBucket="",
             const FileMode mode=FileMode(),
             const std::string & endpoint="",
             const std::string & region="us-east-1",
             const bool anon=false,
             const std::string & accessKey="",
             const std::string & secretKey="")
            : BaseType(mode),
              S3HandleImpl(bucketName, nameInBucket, endpoint, region, anon, accessKey, secretKey){}

        // Implement the handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        // dummy impl (no filesystem path for object storage)
        const fs::path & path() const {static const fs::path p; return p;}

        inline bool isZarr() const {return isZarrGroup();}
        inline bool exists() const {return existsImpl();}

        inline void create() const {
            if(!mode().canCreate()) {
                const std::string err = "Cannot create new file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            // make sure that the file does not exist already
            if(exists()) {
                throw std::invalid_argument("Creating new file failed because it already exists.");
            }
            // nothing to materialize on object storage: the metadata object that
            // the factory writes next is what makes the file "exist".
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove file in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            removeImpl();
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {keysImpl(out);}
        inline bool in(const std::string & key) const {return inImpl(key);}

        Z5_S3_CONFIG_API
        inline std::shared_ptr<Aws::S3::S3Client> makeClient() const {return S3HandleImpl::makeClient();}
    };


    class Group : public z5::handle::Group<Group>, private S3HandleImpl {
    public:
        typedef z5::handle::Group<Group> BaseType;

        template<class GROUP>
        Group(const z5::handle::Group<GROUP> & group, const std::string & key)
            : BaseType(group.mode()),
              S3HandleImpl(group.bucketName(),
                           (group.nameInBucket() == "") ? key : group.nameInBucket() + "/" + key,
                           group.endpoint(), group.region(), group.anon(),
                           group.accessKey(), group.secretKey()){}

        // Implement the handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {return existsImpl();}
        inline bool isZarr() const {return isZarrGroup();}
        const fs::path & path() const {static const fs::path p; return p;}

        inline void create() const {
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new group in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(exists()) {
                throw std::invalid_argument("Creating new group failed because it already exists.");
            }
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove group in group mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            removeImpl();
        }

        // Implement the group handle API
        inline void keys(std::vector<std::string> & out) const {keysImpl(out);}
        inline bool in(const std::string & key) const {return inImpl(key);}

        Z5_S3_CONFIG_API
        inline std::shared_ptr<Aws::S3::S3Client> makeClient() const {return S3HandleImpl::makeClient();}
    };


    class Dataset : public z5::handle::Dataset<Dataset>, private S3HandleImpl {
    public:
        typedef z5::handle::Dataset<Dataset> BaseType;

        template<class GROUP>
        Dataset(const z5::handle::Group<GROUP> & group, const std::string & key,
                const std::string & zarrDelimiter=".", const int zarrFormat=2,
                const std::string & chunkKeyEncoding="default")
            : BaseType(group.mode(), zarrDelimiter, zarrFormat, chunkKeyEncoding),
              S3HandleImpl(group.bucketName(),
                           (group.nameInBucket() == "") ? key : group.nameInBucket() + "/" + key,
                           group.endpoint(), group.region(), group.anon(),
                           group.accessKey(), group.secretKey()){}

        // copy keeps the cached isZarr flag (atomics are not copyable by default)
        Dataset(const Dataset & other)
            : BaseType(other), S3HandleImpl(other),
              isZarrCache_(other.isZarrCache_.load(std::memory_order_relaxed)) {}

        // Implement the handle API
        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}
        inline bool exists() const {return existsImpl();}

        // isZarrDataset() costs 1-2 LIST round trips and is queried for EVERY chunk
        // handle (the chunk-key format depends on it), so the result is cached. The
        // owning s3::Dataset seeds the cache from its metadata via setIsZarr.
        inline bool isZarr() const {
            signed char cached = isZarrCache_.load(std::memory_order_relaxed);
            if(cached < 0) {
                cached = isZarrDataset() ? 1 : 0;
                isZarrCache_.store(cached, std::memory_order_relaxed);
            }
            return cached == 1;
        }
        inline void setIsZarr(const bool isZarr) const {
            isZarrCache_.store(isZarr ? 1 : 0, std::memory_order_relaxed);
        }

        // dummy implementation
        const fs::path & path() const {static const fs::path p; return p;}

        inline void create() const {
            if(mode().mode() == FileMode::modes::r) {
                const std::string err = "Cannot create new dataset in mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            if(exists()) {
                throw std::invalid_argument("Creating new dataset failed because it already exists.");
            }
        }

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove dataset in dataset mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            removeImpl();
        }

        Z5_S3_CONFIG_API
        inline std::shared_ptr<Aws::S3::S3Client> makeClient() const {return S3HandleImpl::makeClient();}

    private:
        // -1: unknown (probe on first use), 0: n5, 1: zarr
        mutable std::atomic<signed char> isZarrCache_{-1};
    };


    class Chunk : public z5::handle::Chunk<Chunk>, private S3HandleImpl {
    public:
        typedef z5::handle::Chunk<Chunk> BaseType;

        Chunk(const Dataset & ds,
              const types::ShapeType & chunkIndices,
              const types::ShapeType & chunkShape,
              const types::ShapeType & shape)
            : BaseType(chunkIndices, chunkShape, shape, ds.mode()),
              dsHandle_(ds),
              S3HandleImpl(ds.bucketName(),
                           ds.nameInBucket() + "/" + getChunkKey(ds.isZarr(), ds.zarrDelimiter(),
                                                                 ds.zarrFormat(), ds.chunkKeyEncoding()),
                           ds.endpoint(), ds.region(), ds.anon(),
                           ds.accessKey(), ds.secretKey()){}

        inline void remove() const {
            if(!mode().canWrite()) {
                const std::string err = "Cannot remove chunk in file mode " + mode().printMode();
                throw std::invalid_argument(err.c_str());
            }
            auto client = makeClient();
            detail::deleteObject(*client, bucketName(), nameInBucket());
        }

        inline const Dataset & datasetHandle() const {return dsHandle_;}

        inline bool isZarr() const {return dsHandle_.isZarr();}
        // chunks are single objects: check the exact key, NOT the prefix
        // (prefix matching would make chunk "1.1" exist whenever "1.10" does)
        inline bool exists() const {
            auto client = makeClient();
            return detail::objectExists(*client, bucketName(), nameInBucket());
        }

        // dummy impl
        const fs::path & path() const {static const fs::path p; return p;}

        inline void create() const {}

        inline bool isS3() const {return true;}
        inline bool isGcs() const {return false;}

        Z5_S3_CONFIG_API
        inline std::shared_ptr<Aws::S3::S3Client> makeClient() const {return S3HandleImpl::makeClient();}

    private:
        const Dataset & dsHandle_;
    };

    #undef Z5_S3_CONFIG_API

}
}
}
