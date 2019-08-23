#include "gtest/gtest.h"
#include "z5/filesystem/handle.hxx"

namespace z5 {

    // fixture for the handle test
    class HandleTest : public ::testing::Test {

    protected:
        HandleTest() : tmp("tmp_dir"){}

        ~HandleTest(){
        }

        void SetUp() {
            fs::create_directories(tmp);
        }

        void TearDown() {
            fs::remove_all(tmp);
        }

        fs::path tmp;
    };


    TEST_F(HandleTest, TestFile) {
        // Make a file
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File f(p);
        f.create();
        ASSERT_TRUE(f.exists());
    }


    TEST_F(HandleTest, TestGroup) {
        // Make a file
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File f(p);

        // Make a group
        z5::filesystem::handle::Group g(f, "group");
        g.create();
        ASSERT_TRUE(g.exists());

        // Make a sub-group
        z5::filesystem::handle::Group g1(g, "sub");
        g1.create();
        ASSERT_TRUE(g1.exists());

        // check the keys
        std::vector<std::string> keys;
        g.keys(keys);
        ASSERT_EQ(keys.size(), 1);
        ASSERT_EQ(keys[0], "sub");

        // check membership
        ASSERT_TRUE(f.in("group"));
        ASSERT_TRUE(f.in("group/sub"));
        ASSERT_FALSE(f.in("blub"));
        ASSERT_FALSE(f.in("group/no"));

        ASSERT_TRUE(g.in("sub"));
        ASSERT_FALSE(g.in("nono"));
    }


    TEST_F(HandleTest, TestDataset) {

        // Make a file
        fs::path p = tmp / "f.n5";
        z5::filesystem::handle::File f(p);

        // Make a dataset
        z5::filesystem::handle::Dataset ds(f, "ds");
        ds.create();
        ASSERT_TRUE(ds.exists());

        // Make a group
        z5::filesystem::handle::Group g(f, "group");

        // Make a dataset
        z5::filesystem::handle::Dataset ds1(g, "ds");
        ds1.create();
        ASSERT_TRUE(ds1.exists());

        // check the keys
        std::vector<std::string> keys;
        g.keys(keys);
        ASSERT_EQ(keys.size(), 1);
        ASSERT_EQ(keys[0], "ds");

        // check membership
        ASSERT_TRUE(f.in("ds"));
        ASSERT_TRUE(f.in("group/ds"));
        ASSERT_TRUE(g.in("ds"));
    }
}
