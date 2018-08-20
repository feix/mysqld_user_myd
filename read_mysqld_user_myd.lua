-- https://dev.mysql.com/doc/internals/en/layout-record-storage-frame.html

local read_len = function(content, first, l)
    local sum = 0
    local last = first + l - 1
    for i = first, last do
        sum = sum * 256 + content[i]
    end
    return sum
end

local min = function(a, b)
    return a <= b and a or b
end

local pad = function(data_len)
    local t_len = data_len % 4
    if t_len == 0 then
        return data_len
    else
        return data_len - t_len + 4
    end
end

local dispatch_record
local read_record = function(content, idx, header_len, data_pos, data_l, next_pos, unused_l)
-- block
--  header
--  data with pad
    local rec_type = content[idx]
    local data_len = read_len(content, idx+data_pos, data_l)
    local unused_len = unused_l > 0 and content[idx+unused_l] or 0
    local block_len = pad(header_len + data_len + unused_len)
    local next_rec = next_pos > 0 and dispatch_record(content, read_len(content, idx+next_pos, 8)+1) or nil
    return {
        rec_type=rec_type,
        block_len=block_len,
        data_len=data_len,
        next_rec=next_rec,
        data_begin=idx + header_len
    }
end

dispatch_record = function(content, idx)
    local record = {}
    local rec_type = content[idx]
    if rec_type == 0 then
        record = read_record(content, idx, 20, 1, 3, -1, 0)
    elseif rec_type == 1 then
        record = read_record(content, idx, 3, 1, 2, -1, 0)
    elseif rec_type == 2 then
        record = read_record(content, idx, 4, 1, 3, -1, 0)
    elseif rec_type == 3 then
        record = read_record(content, idx, 4, 1, 2, -1, 3)
    elseif rec_type == 4 then
        record = read_record(content, idx, 5, 1, 3, -1, 4)
    elseif rec_type == 5 then
        record = read_record(content, idx, 13, 3, 2, 5, 0)
    elseif rec_type == 6 then
        record = read_record(content, idx, 15, 4, 3, 7, 0)
    elseif rec_type == 7 then
        record = read_record(content, idx, 3, 1, 2, -1, 0)
    elseif rec_type == 8 then
        record = read_record(content, idx, 4, 1, 3, -1, 0)
    elseif rec_type == 9 then
        record = read_record(content, idx, 4, 1, 2, -1, 3)
    elseif rec_type == 10 then
        record = read_record(content, idx, 5, 1, 3, -1, 4)
    elseif rec_type == 11 then
        record = read_record(content, idx, 11, 1, 2, 3, 0)
    elseif rec_type == 12 then
        record = read_record(content, idx, 12, 1, 3, 4, 0)
    elseif rec_type == 13 then
        record = read_record(content, idx, 16, 5, 3, 9, 0)
    end
    return record
end

local read_str = function(content, first, l)
    local str = {}
    local last = first + l - 1
    for i = first, last do
        table.insert(str, string.char(content[i]))
    end
    return table.concat(str)
end

local parse_record = function(content, rec)
    local first = rec.data_begin + 3
    local host_l = content[first]
    local host = read_str(content, first+1, host_l)

    local user_l = content[first+host_l+1]
    local user = read_str(content, first+host_l+1+1, user_l)

    local native = false
    local pass_l = 40
    local pass = ''
    local idx = first + host_l + 1 + 1 + user_l
    while true do
        local n_pass_l = string.len(pass)
        local last = rec.data_begin + rec.data_len
        if n_pass_l == 0 then
            while idx < last do
                if content[idx] == 21 then
                    native = true
                elseif content[idx] == 42 then
                    idx = idx + 1
                    break
                end
                idx = idx + 1
            end
        end
        local n_pass = read_str(content, idx, min(last-idx, pass_l-n_pass_l))
        pass = pass .. n_pass
        if rec.next_rec == nil then
            break
        else
            rec = rec.next_rec
            idx = rec.data_begin
        end
    end

    return {
        host=host,
        user=user,
        pass=pass,
        native=native
    }
end

local read_records = function(filename)
    local content = {}
    local content_len = 0
    local file = io.open(filename, 'rb')
    while file ~= nil do
        local buffer = file:read(1024)
        if buffer == nil then
            break
        end
        for i = 1, #buffer do
            content[content_len+i] = buffer:byte(i)
        end
        content_len = content_len + #buffer
    end
    if file ~= nil then
        file:close()
    end

    local idx = 1
    while idx <= content_len do
        local rec = dispatch_record(content, idx)
        if 0 < rec.rec_type and rec.rec_type <= 6 then
            local record = parse_record(content, rec)
            print(record.host, record.user, record.pass, string.len(record.pass), record.native)
        end
        idx = idx + rec.block_len
    end
    return record
end

read_records('/usr/local/var/mysql/mysql/user.MYD')
