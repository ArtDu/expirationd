local expirationd = require("expirationd")
local t = require("luatest")
local g = t.group("custom_index")

local helpers = require("test.helper")

g.before_all(function()
    helpers.init_spaces(g)
end)

g.after_each(function()
    helpers.truncate_spaces(g)
end)

function g.test_passing()
    local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true)
    -- if we don't specify index, program should use primary index
    t.assert_equals(task.index, box.space.tree.index[0])
    task:kill()

    -- index by name
    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {index = "index_for_first_name"})
    t.assert_equals(task.index, box.space.tree.index[1])
    task:kill()

    -- index by id
    task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_true,
            {index = 1})
    t.assert_equals(task.index, box.space.tree.index[1])
    task:kill()

    -- errors
    t.assert_error_msg_content_equals("Index with name not_exists_index does not exist",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            {index = "not_exists_index"})
    t.assert_error_msg_content_equals("Index with id 10 does not exist",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            {index = 10})
    t.assert_error_msg_content_equals("Invalid type of index, expected string or number",
            expirationd.start, "clean_all", g.tree.id, helpers.is_expired_true,
            {index = { 10 }})
end

function g.test_tree_index()
    for _, space in pairs({g.tree, g.vinyl}) do
        helpers.iteration_result = {}

        space:insert({1, "3"})
        space:insert({2, "2"})
        space:insert({3, "1"})

        -- check default primary index
        local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug)
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {1, "3"},
                {2, "2"},
                {3, "1"}
            })
        end)
        task:kill()
        helpers.iteration_result = {}

        space:insert({1, "3"})
        space:insert({2, "2"})
        space:insert({3, "1"})

        -- check custom index
        task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                {index = "index_for_first_name"})
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {3, "1"},
                {2, "2"},
                {1, "3"}
            })
        end)
        task:kill()
    end
end

function g.test_tree_index_non_unique()
    for _, space in pairs({g.tree, g.vinyl}) do
        helpers.iteration_result = {}

        space:insert({1, "3", nil, nil, 1})
        space:insert({2, "2", nil, nil, 2})
        space:insert({3, "1", nil, nil, 1})

        local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                {index = "non_unique_index"})
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                        {1, "3", nil, nil, 1},
                        {3, "1", nil, nil, 1},
                        {2, "2", nil, nil, 2}
                    })
        end)
        task:kill()
    end
end

function g.test_tree_index_multipart()
    for _, space in pairs({g.tree, g.vinyl}) do
        helpers.iteration_result = {}

        space:insert({1, "1", 2, 1})
        space:insert({2, "2", 2, 2})
        space:insert({3, "3", 1, 3})

        local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                {index = "multipart_index"})
        -- wait for tuples expired
        helpers.retrying({}, function()
            t.assert_equals(helpers.iteration_result, {
                {3, "3", 1, 3},
                {1, "1", 2, 1},
                {2, "2", 2, 2}
            })
        end)
        task:kill()
    end
end

if _TARANTOOL >= "2" then
    function g.test_tree_index_json_path()
        for _, space in pairs({g.tree, g.vinyl}) do
            helpers.iteration_result = {}

            space:insert({1, "1", nil, nil, nil, { age  = 3 }})
            space:insert({2, "2", nil, nil, nil, { age  = 1 }})
            space:insert({3, "3", nil, nil, nil, { age  = 2 }})
            space:insert({4, "4", nil, nil, nil, { days = 3 }})
            space:insert({5, "5", nil, nil, nil, { days = 1 }})
            space:insert({6, "6", nil, nil, nil, { days = 2 }})


            local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                    {index = "json_path_index"})
            -- wait for tuples expired
            helpers.retrying({}, function()
                t.assert_equals(helpers.iteration_result, {
                    {4, "4", nil, nil, nil, { days = 3 }},
                    {5, "5", nil, nil, nil, { days = 1 }},
                    {6, "6", nil, nil, nil, { days = 2 }},
                    {2, "2", nil, nil, nil, { age = 1 }},
                    {3, "3", nil, nil, nil, { age = 2 }},
                    {1, "1", nil, nil, nil, { age = 3 }}
                })
            end)
            task:kill()
        end
    end

    function g.test_tree_index_multikey()
        for _, space in pairs({g.tree, g.vinyl}) do
            helpers.iteration_result = {}

            space:insert({1, "1", nil, nil, nil, nil, {data = {{name = "A"},
                                                               {name = "B"}},
                                                       extra_field = 1}})

            local task = expirationd.start("clean_all", space.id, helpers.is_expired_debug,
                    {index = "multikey_index"})
            -- wait for tuples expired
            helpers.retrying({}, function()
                -- met only once, since we delete and cannot walk a second time on name = "B"
                t.assert_equals(helpers.iteration_result, {
                    {1, "1", nil, nil, nil, nil, {data = {{name = "A"},
                                                          {name = "B"}},
                                                  extra_field = 1}}
                })
            end)
            task:kill()
        end
    end

    function g.test_memtx_tree_functional_index()
        -- vinyl doesn't support functional indexes
        helpers.iteration_result = {}

        g.tree:insert({1, "1", nil, nil, nil, nil, nil, "12"})
        g.tree:insert({2, "2", nil, nil, nil, nil, nil, "21"})

        local task = expirationd.start("clean_all", g.tree.id, helpers.is_expired_debug,
                {index = "functional_index"})
        -- wait for tuples expired
        helpers.retrying({}, function()
            -- sort by second character to eighth field
            t.assert_equals(helpers.iteration_result, {
                {2, "2", nil, nil, nil, nil, nil, "21"},
                {1, "1", nil, nil, nil, nil, nil, "12"}
            })
        end)
        task:kill()
    end
end


function g.test_hash_index()
    helpers.iteration_result = {}
    g.hash:insert({1, "a"})
    g.hash:insert({2, "b"})
    g.hash:insert({3, "c"})

    -- check default primary index
    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug)
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {3, "c"},
            {2, "b"},
            {1, "a"}
        })
    end)
    task:kill()

    helpers.iteration_result = {}
    g.hash:insert({1, "a"})
    g.hash:insert({2, "b"})
    g.hash:insert({3, "c"})

    task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {index = "index_for_first_name"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {1, "a"},
            {3, "c"},
            {2, "b"}
        })
    end)
    helpers.iteration_result = {}
    task:kill()
end

function g.test_hash_index_multipart()
    helpers.iteration_result = {}

    g.hash:insert({1, "1"})
    g.hash:insert({2, "2"})
    g.hash:insert({3, "3"})

    local task = expirationd.start("clean_all", g.hash.id, helpers.is_expired_debug,
            {index = "multipart_index"})
    -- wait for tuples expired
    helpers.retrying({}, function()
        t.assert_equals(helpers.iteration_result, {
            {2, "2"},
            {1, "1"},
            {3, "3"}
        })
    end)
    task:kill()
end