local expirationd = require('expirationd')
local fiber = require('fiber')
local t = require('luatest')

local helpers = require('test.helper')

local g = t.group('expiration_process', {
    {index_type = 'TREE', engine = 'vinyl'},
    {index_type = 'TREE', engine = 'memtx'},
    {index_type = 'HASH', engine = 'memtx'},
})

g.before_each({index_type = 'TREE'}, function(cg)
    t.skip_if(cg.params.engine == 'vinyl' and not helpers.vinyl_is_supported(),
        'Blocked by https://github.com/tarantool/tarantool/issues/6448')
    g.space = helpers.create_space_with_tree_index(cg.params.engine)
end)

g.before_each({index_type = 'HASH'}, function(cg)
    g.space = helpers.create_space_with_hash_index(cg.params.engine)
end)

g.before_each(function(cg)
    local space_archive = helpers.create_space('archived_tree', cg.params.engine)
    space_archive:create_index('primary')
    g.space_archive = space_archive

    cg.task_name = 'test'
end)

g.after_each(function(g)
    if g.task ~= nil then
        g.task:kill()
    end
    g.space:drop()
    g.space_archive:drop()
end)

-- Check tuple's expiration by timestamp.
local function check_tuple_expire_by_timestamp(args, tuple)
    local tuple_expire_time = tuple[args.field_no]

    local current_time = fiber.time()
    return current_time >= tuple_expire_time
end

-- Put expired tuple in archive.
local function put_tuple_to_archive(space_id, args, tuple)
    -- Delete expired tuple.
    box.space[space_id]:delete({tuple.id})
    local id, first_name = tuple.id, tuple.first_name
    if args.archive_space_id ~= nil and id ~= nil and first_name ~= nil then
        box.space[args.archive_space_id]:insert({id, first_name, fiber.time()})
    end
end

-- Checking that we can use custom is_tuple_expired, process_expired_tuple,
-- these basic functions are included in expiration_process.
-- We also test the timestamp expiration check.
function g.test_archive_by_timestamp(cg)
    local space = cg.space
    local space_archive = cg.space_archive
    local task_name = cg.task_name

    local total = 10
    local todelete = 5
    local time = fiber.time()
    local deleted = {}
    local nondeleted = {}
    for i = 1, total do
        local tuple
        if i <= todelete then
            -- This tuples should be deleted by the expirationd.
            tuple = {i, tostring(i), time}
            table.insert(deleted, tuple)
        else
            -- This tuples should still exist.
            tuple = {i, tostring(i), time + 60}
            table.insert(nondeleted, tuple)
        end
        space:insert(tuple)
    end

    cg.task = expirationd.start(task_name, space.id,
        check_tuple_expire_by_timestamp,
        {
            process_expired_tuple = put_tuple_to_archive,
            args = {
                field_no = 3,
                archive_space_id = space_archive.id
            },
        })
    local task = cg.task
    local start_time = fiber.time()

    -- We sure that the task will be executed.
    helpers.retrying({}, function()
        t.assert_equals(space_archive:count(), #deleted)
    end)

    -- Check the validity of the task parameters.
    t.assert_equals(task.name, 'test')
    t.assert_equals(task.name, task_name)
    t.assert_equals(task.start_time, start_time)
    t.assert_equals(task.restarts, 1)

    -- Check tuple processing.
    t.assert_equals(space_archive:count(), #deleted)
    t.assert_equals(task.expired_tuples_count, #deleted)
    t.assert_items_include(space:select(nil, {limit = 1000}), nondeleted)
end
