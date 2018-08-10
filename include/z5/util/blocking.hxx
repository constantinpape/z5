#pragma once

#include "z5/types/types.hxx"


namespace z5 {
namespace util {

    class Blocking {
    public:
        // TODO support offset
        Blocking(const types::ShapeType & shape,
                 const types::ShapeType & blockShape) : shape_(shape), blockShape_(blockShape) {
            init();
        }

        //
        // get member variables
        //
        inline size_t numberOfBlocks() const {return numberOfBlocks_;}
        inline const types::ShapeType & blocksPerDimension() const {return blocksPerDimension_;}
        inline const types::ShapeType & shape() const {return shape_;}

        //
        // block id to block coordinate and vice versa
        //
        inline void blockIdToBlockCoordinate(const size_t blockId,
                                             types::ShapeType & blockCoordinate) const {
            blockCoordinate.resize(shape_.size());
            std::size_t index = blockId;
            for(unsigned d = 0; d < shape_.size(); ++d) {
                blockCoordinate[d] = index / blockStrides_[d];
                index -= blockCoordinate[d] * blockStrides_[d];
            }
        }

        // TODO
        inline size_t blockCoordinatesToBlockId(const types::ShapeType & blockCoordinate) const {

        }

        //
        // coordinates of given block
        //
        inline void getBlockBeginAndShape(const size_t blockId,
                                          types::ShapeType & blockBegin,
                                          types::ShapeType & blockShape) const {
            types::ShapeType blockCoordinate;
            blockIdToBlockCoordinate(blockId, blockCoordinate);
            getBlockBeginAndShape(blockCoordinate, blockBegin, blockShape);
        }

        inline void getBlockBeginAndShape(const types::ShapeType & blockCoordinate,
                                          types::ShapeType & blockBegin,
                                          types::ShapeType & blockShape) const {
        }

        inline void getBlockBeginAndEnd(const size_t blockId,
                                        types::ShapeType & blockBegin,
                                        types::ShapeType & blockEnd) const {
            types::ShapeType blockCoordinate;
            blockIdToBlockCoordinate(blockId, blockCoordinate);
            getBlockBeginAndEnd(blockCoordinate, blockBegin, blockEnd);
        }

        inline void getBlockBeginAndEnd(const types::ShapeType & blockCoordinate,
                                        types::ShapeType & blockBegin,
                                        types::ShapeType & blockEnd) const {
        }

        //
        // overlap of blocks and rois
        //

    private:
        void init() {
            const unsigned ndim = shape_.size();
            blocksPerDimension_.resize(ndim);

            for(int d = 0; d < ndim; ++d) {
                blocksPerDimension_[d] = shape_[d] / blockShape_[d] + (shape_[d] % blockShape_[d] == 0 ? 0 : 1);
            }
            numberOfBlocks_ = std::accumulate(blocksPerDimension_.begin(),
                                              blocksPerDimension_.end(),
                                              1, std::multiplies<std::size_t>());

            // get the block strides
            blockStrides_.resize(ndim);
            blockStrides_[ndim - 1] = 1;
            for(int d = ndim - 2; d >= 0; --d) {
                blockStrides_[d] = blockStrides_[d + 1] * blocksPerDimension_[d + 1];
            }
        }

        types::ShapeType shape_;
        types::ShapeType blockShape_;
        types::ShapeType blocksPerDimension_;
        types::ShapeType blockStrides_;
        size_t numberOfBlocks_;
    };


}
}
