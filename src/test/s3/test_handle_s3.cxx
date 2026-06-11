#include <chrono>
#include <cstdlib>
#include <string>

#include "gtest/gtest.h"

#include <aws/s3/model/CreateBucketRequest.h>

#include "z5/s3/handle.hxx"

// These tests need a reachable S3 endpoint (a local moto server is enough):
//   pip install 'moto[server]' && moto_server -p 5000 &
//   export Z5PY_S3_ENDPOINT=http://localhost:5000   # same variable as the python tests
// Without the endpoint they skip.

namespace z5 {

namespace {

    constexpr const char * TEST_BUCKET = "z5-test-handle-bucket";

    inline std::string testEndpoint() {
        const char * ep = std::getenv("Z5PY_S3_ENDPOINT");
        return ep == nullptr ? "" : std::string(ep);
    }

    // a per-test unique prefix so tests don't see each other's objects
    inline std::string uniquePrefix(const std::string & name) {
        const auto t = std::chrono::steady_clock::now().time_since_epoch().count();
        return name + "-" + std::to_string(t);
    }

    inline s3::handle::File makeFile(const std::string & endpoint, const std::string & prefix) {
        return s3::handle::File(TEST_BUCKET, prefix, FileMode(), endpoint,
                                "us-east-1", false, "testing", "testing");
    }

    inline void ensureBucket(const s3::handle::File & f) {
        auto client = f.makeClient();
        Aws::S3::Model::CreateBucketRequest request;
        request.SetBucket(TEST_BUCKET);
        // already-exists outcomes are fine, anything else surfaces in the tests
        client.CreateBucket(request);
    }

}

    class HandleTest : public ::testing::Test {
    protected:
        void SetUp() override {
            endpoint_ = testEndpoint();
        }

        // skip when no test endpoint is configured (never run against real AWS)
        bool skipWithoutEndpoint() {
            return endpoint_.empty();
        }

        std::string endpoint_;
    };


    TEST_F(HandleTest, TestFile) {
        if(skipWithoutEndpoint()) GTEST_SKIP() << "Z5PY_S3_ENDPOINT not set";
        auto f = makeFile(endpoint_, uniquePrefix("file"));
        ensureBucket(f);

        EXPECT_FALSE(f.exists());
        f.create();

        // the file only materializes once an object is written under its prefix
        auto client = f.makeClient();
        s3::detail::putObjectString(client, f.bucketName(),
                                    s3::detail::joinKey(f.nameInBucket(), ".zgroup"),
                                    "{\"zarr_format\": 2}");
        EXPECT_TRUE(f.exists());
        f.remove();
        EXPECT_FALSE(f.exists());
    }


    TEST_F(HandleTest, TestGroup) {
        if(skipWithoutEndpoint()) GTEST_SKIP() << "Z5PY_S3_ENDPOINT not set";
        auto f = makeFile(endpoint_, uniquePrefix("group"));
        ensureBucket(f);
        auto client = f.makeClient();

        s3::handle::Group g(f, "group");
        EXPECT_FALSE(g.exists());
        g.create();
        s3::detail::putObjectString(client, g.bucketName(),
                                    s3::detail::joinKey(g.nameInBucket(), ".zgroup"),
                                    "{\"zarr_format\": 2}");
        EXPECT_TRUE(g.exists());
        EXPECT_TRUE(f.in("group"));

        s3::handle::Group g1(g, "sub");
        EXPECT_FALSE(g1.exists());
        g1.create();

        f.remove();
    }


    TEST_F(HandleTest, TestDataset) {
        if(skipWithoutEndpoint()) GTEST_SKIP() << "Z5PY_S3_ENDPOINT not set";
        auto f = makeFile(endpoint_, uniquePrefix("ds"));
        ensureBucket(f);

        s3::handle::Dataset ds(f, "ds");
        EXPECT_FALSE(ds.exists());
        ds.create();

        s3::handle::Group g(f, "group");
        s3::handle::Dataset ds1(g, "ds");
        EXPECT_FALSE(ds1.exists());
        ds1.create();

        f.remove();
    }


    // regression test: existence checks must not treat sibling names that merely
    // share a prefix as the same object ("data" vs "data2", chunk "1.1" vs "1.10")
    TEST_F(HandleTest, TestExistsNoPrefixFalsePositives) {
        if(skipWithoutEndpoint()) GTEST_SKIP() << "Z5PY_S3_ENDPOINT not set";
        auto f = makeFile(endpoint_, uniquePrefix("prefix"));
        ensureBucket(f);
        auto client = f.makeClient();

        // only "data2" exists
        s3::detail::putObjectString(client, f.bucketName(),
                                    s3::detail::joinKey(f.nameInBucket(), "data2/zarr.json"),
                                    "{}");

        EXPECT_TRUE(f.in("data2"));
        EXPECT_FALSE(f.in("data"));

        s3::handle::Dataset existing(f, "data2");
        EXPECT_TRUE(existing.exists());
        s3::handle::Dataset fresh(f, "data");
        EXPECT_FALSE(fresh.exists());
        // creating "data" must not fail with "already exists"
        EXPECT_NO_THROW(fresh.create());

        f.remove();
    }

}
