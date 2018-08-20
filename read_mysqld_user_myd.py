#!/usr/bin/env python3
# https://dev.mysql.com/doc/internals/en/layout-record-storage-frame.html


def read_len(content, begin, l):
    sum_len = 0
    for bit in content[begin:begin+l]:
        sum_len = (sum_len << 8) + bit
    return sum_len


# 4 bits padding 1 bytes
def pad(data_len):
    byte_len = data_len >> 2
    return (byte_len + ((data_len - (byte_len << 2)) & 1)) << 2


def read_record(content, idx, header_len, data_pos, data_l, next_pos=-1, unused_l=0):
    rec_type = content[idx]
    data_len = read_len(content, idx+data_pos, data_l)
    unused_len = unused_l > 0 and content[idx+unused_l] or 0
    block_len = pad(header_len + data_len + unused_len)
    next_rec = next_pos > 0 and dispatch_record(content, read_len(content, idx+next_pos, 8)) or None
    return dict(
        rec_type=rec_type,
        block_len=block_len,
        data_len=data_len,
        next_rec=next_rec,
        data_begin=idx + header_len
    )


def dispatch_record(content, idx):
    rec_type = content[idx]
    if rec_type == 0:
        record = read_record(content, idx, 20, 1, 3)
    elif rec_type == 1:
        record = read_record(content, idx, 3, 1, 2)
    elif rec_type == 2:
        record = read_record(content, idx, 4, 1, 3)
    elif rec_type == 3:
        record = read_record(content, idx, 4, 1, 2, unused_l=3)
    elif rec_type == 4:
        record = read_record(content, idx, 5, 1, 3, unused_l=4)
    elif rec_type == 5:
        record = read_record(content, idx, 13, 3, 2, 5)
    elif rec_type == 6:
        record = read_record(content, idx, 15, 4, 3, 7)
    elif rec_type == 7:
        record = read_record(content, idx, 3, 1, 2)
    elif rec_type == 8:
        record = read_record(content, idx, 4, 1, 3)
    elif rec_type == 9:
        record = read_record(content, idx, 4, 1, 2, unused_l=3)
    elif rec_type == 10:
        record = read_record(content, idx, 5, 1, 3, unused_l=4)
    elif rec_type == 11:
        record = read_record(content, idx, 11, 1, 2, 3)
    elif rec_type == 12:
        record = read_record(content, idx, 12, 1, 3, 4)
    elif rec_type == 13:
        record = read_record(content, idx, 16, 5, 3, 9)
    return record


def parse_record(content, rec):
    first = rec['data_begin'] + 3
    host_l = content[first]
    host = content[first+1:first+1+host_l]

    user_l = content[first+host_l+1]
    user = content[first+host_l+1+1:first+host_l+1+1+user_l]

    native = False
    password_l = 40
    password = []
    idx = first + host_l + 1 + 1 + user_l
    while True:
        last = rec['data_begin'] + rec['data_len']
        password_curl_l = len(password)
        if password_curl_l == 0:
            while idx < last:
                if content[idx] == 21:
                    native = True
                elif content[idx] == 42:
                    break
                idx += 1
        password += content[idx+1:min(last, password_l-password_curl_l+idx+1)]
        if not rec['next_rec']:
            break
        else:
            rec = rec['next_rec']
            idx = rec['data_begin']
    return dict(
        host=host,
        user=user,
        password=''.join(map(chr, password)),
        native=native
    )


def read_records(filename):
    with open(filename, 'rb') as f:
        content = f.read()
        content_len = len(content)
        idx = 0
        while idx < content_len:
            record = dispatch_record(content, idx)
            if 0 < record['rec_type'] <= 6:
                print(idx, record['rec_type'], parse_record(content, record))
            idx += record['block_len']


def main():
    read_records('/usr/local/var/mysql/mysql/user.MYD')


if __name__ == '__main__':
    main()
