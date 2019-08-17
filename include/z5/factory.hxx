#pragma once

#include "z5/filesystem/factory.hxx"

#ifdef WITH_S3
#include "z5/s3/factory.hxx"
#endif


namespace z5 {


    template<class GROUP>
    inline std::unique_ptr<Dataset> openDataset(const handle::Group<GROUP> & root,
                                                const std::string & key) {

        // check if this is a s3 group
        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Dataset ds(root, key);
            return s3::openDataset(ds);
        }
        #endif

        filesystem::handle::Dataset ds(root, key);
        return filesystem::openDataset(ds);
    }


    /*
    template<class DATASET>
    inline std::unique_ptr<Dataset> openDataset(const handle::Dataset<DATASET> & ds) {

        // check if this is a s3 dataset
        #ifdef WITH_S3
        if(ds.isS3()) {
            return s3::openDataset(ds);
        }
        #endif

        return filesystem::openDataset(ds);
    }
    */


    template<class GROUP>
    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group<GROUP> & root,
        const std::string & key,
        const DatasetMetadata & metadata
    ) {
        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Dataset ds(root, key);
            return s3::createDataset(ds, metadata);
        }
        #endif

        filesystem::handle::Dataset ds(root, key);
        return filesystem::createDataset(ds, metadata);
    }


    template<class GROUP>
    inline std::unique_ptr<Dataset> createDataset(
        const handle::Group<GROUP> & root,
        const std::string & key,
        const std::string & dtype,
        const types::ShapeType & shape,
        const types::ShapeType & chunkShape,
        const std::string & compressor="raw",
        const types::CompressionOptions & compressionOptions=types::CompressionOptions(),
        const double fillValue=0
    ) {
        DatasetMetadata metadata;
        createDatasetMetadata(dtype, shape, chunkShape, root.isZarr(),
                              compressor, compressionOptions, fillValue,
                              metadata);

        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Dataset ds(root, key);
            return s3::createDataset(ds, metadata);
        }
        #endif

        filesystem::handle::Dataset ds(root, key);
        return filesystem::createDataset(ds, metadata);
    }


    template<class GROUP>
    inline void createFile(const handle::File<GROUP> & file, const bool isZarr) {
        #ifdef WITH_S3
        if(file.isS3()) {
            s3::createFile(file, isZarr);
            return;
        }
        #endif
        filesystem::createFile(file, isZarr);
    }


    // TODO return the handle to the group that was created here.
    // need to return it wrapped into a unique ptr, to enable polymorphism
    template<class GROUP>
    inline void createGroup(const handle::Group<GROUP> & root, const std::string & key) {
        #ifdef WITH_S3
        if(root.isS3()) {
            s3::handle::Group newGroup(root, key);
            s3::createGroup(newGroup, root.isZarr());
            return;
        }
        #endif
        filesystem::handle::Group newGroup(root, key);
        filesystem::createGroup(newGroup, root.isZarr());
    }

}
