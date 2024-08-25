const Redis = require('ioredis')

/**
 * Returns JSON-parsed input string `s` if it is a string,
 * or the value of `s` itself if it is a number, boolean, or null,
 * or the value of `dflt` if none of the above.
 * @param {string|number|boolean|null} s - the input string to parse
 * @param {*} dflt - the default value to return if the input is not a string
 * @returns {Object} the parsed JSON object
 */
const parseJson = function (s, dflt) {
    if (typeof s === 'string') return JSON.parse(s)
    if (['number', 'boolean'].includes(typeof s) || s === null) return s
    return dflt
}

class RedisUtilFunctions {
    /**
     * @param {Redis|Redis.Cluster|Object|string} [client_or_settings] - the Redis client, or settings object, or string connection string
     * @param {boolean} [is_cluster] - whether the Redis server is a cluster
     * @param {string} [prefix] - the string prefix to prepend to every Redis key
     */
    constructor(client_or_settings, is_cluster, prefix) {
        this.redisClient = null
        this.open(client_or_settings, is_cluster)
        this.redisHprefix = prefix || process.env.REDIS_HPREFIX || ''
    }

    /**
     * Sets the Redis client instance.
     * @param {Redis|Redis.Cluster} client - the Redis client instance
     * @returns {boolean} true if the client is set successfully, false otherwise
     */
    setCLient(client) {
        if (client instanceof Redis || client instanceof Redis.Cluster) {
            this.redisClient = client
            return true
        }
        return false
    }

    /**
     * Opens a connection to the Redis server.
     * If the connection is not opened already, it opens a new connection to the Redis server.
     * @param {Redis|Redis.Cluster|Object|string} [client_or_settings] - the Redis client, or settings object, or string connection string
     * @param {boolean} [is_cluster] - whether the Redis server is a cluster
     * @returns {boolean} true if the connection is opened successfully, false otherwise
     */
    open(client_or_settings, is_cluster) {
        if (client_or_settings && this.setCLient(client_or_settings))
            return true
        if (!client_or_settings && process.env.REDIS_CONNECTION)
            client_or_settings = process.env.REDIS_CONNECTION
        if (!client_or_settings) return false
        if (is_cluster === undefined && Number(process.env.REDIS_CLUSTER))
            is_cluster = true
        if (is_cluster) {
            this.redisClient = new Redis.Cluster([client_or_settings])
        } else {
            this.redisClient = new Redis(client_or_settings)
        }
        return true
    }

    /**
     * Async Calls a Redis method, prefixing the key with the stored prefix.
     * If the last argument is a function, it is treated as a callback.
     * If the callback is given, the function returns a Promise,
     * and the callback is called with the result of the Redis call inside the Promise.
     * @see {@link r} for the Sync version
     * @async
     * @param {string} redis_method - the Redis method to call
     * @param {string} hkey - the Redis key to use
     * @param {...*} rest_args - the arguments to pass to the Redis method
     * @returns {Promise|*} the result of the Redis method, or a Promise resolved with the result
     */
    rr(redis_method, hkey, ...rest_args) {
        hkey = this._rpfx(hkey)
        let cb =
            rest_args.length > 0 &&
            typeof rest_args[rest_args.length - 1] === 'function'
                ? rest_args.pop()
                : null
        if (!cb) return this._redis_call(redis_method, hkey, ...rest_args)

        return new Promise(async (resolve, reject) => {
            let f = (err, result) => {
                if (err) return reject(err)
                return resolve(cb(result))
            }
            rest_args.push(f)
            this._redis_call(redis_method, hkey, ...rest_args)
        })
    }

    /**
     * Sync Calls a Redis method, prefixing the key with the stored prefix.
     * If the last argument is not a function, it appends an empty function to the arguments.
     * @see {@link rr} for the Async version
     * @param {string} redis_method - the Redis method to call
     * @param {string} hkey - the Redis key to use
     * @param {...*} rest_args - the arguments to pass to the Redis method
     * @returns {*} the result of the Redis method
     */
    r(redis_method, hkey, ...rest_args) {
        hkey = this._rpfx(hkey)
        if (
            !rest_args.length ||
            typeof rest_args[rest_args.length - 1] !== 'function'
        )
            rest_args.push(function () {})
        return this._redis_call(redis_method, hkey, ...rest_args)
    }

    /**
     * If the given commands array contains only one command, call rr() on it.
     * Otherwise, call rpipemulti() on the commands array.
     * @see {@link rr}
     * @see {@link rpipemulti}
     * @async
     * @param {Array} commands - the commands array to process
     * @returns {Promise|*} the result of the Redis method, or a Promise resolved with the result
     */
    rpipemaybe(commands) {
        return commands.length === 1
            ? this.rr(...commands[0])
            : this.rpipemulti(commands)
    }

    /**
     * Calls multiple Redis methods in a pipeline or a transaction,
     * prefixing the key with the stored prefix.
     * @async
     * @param {Array} commands - the commands array to process
     * @param {string|function} [arg2] - 't' for transaction, or a callback function to call with the results
     * @param {function} [cb] - callback function to call with the results
     * @returns {Promise|*} the result of the Redis method, or a Promise resolved with the result
     */
    rpipemulti(commands, arg2, cb) {
        let type = 'pipeline'
        if (arg2) {
            if (typeof arg2 === 'function') {
                cb = arg2
            } else {
                if (arg2 === 't') type = 'multi'
                if (cb && typeof cb !== 'function') cb = null
            }
        }

        let rpipe = this.redisClient[type]()
        for (let i = 0; i < commands.length; ++i) {
            if (commands[i].length > 1)
                commands[i][1] = this._rpfx(commands[i][1])
            commands[i].push(rpipe)
            this._redis_call(...commands[i])
        }

        let ret = cb
            ? rpipe.exec((err, results) => cb(err, results))
            : rpipe.exec()
        return ret
    }

    /**
     * Calls multiple Redis methods in a pipeline, prefixing the key with the stored prefix.
     * Returns a Promise resolved with an array of results, where each result is the result of the corresponding Redis method.
     * @see {@link rpipemulti}
     * @async
     * @param {Array} commands - the commands array to process
     * @returns {Promise<Array>} the result of the Redis method, or a Promise resolved with the result
     */
    async rpipemulti2array(commands) {
        let rpipe = this.redisClient.pipeline()
        for (let i = 0; i < commands.length; ++i) {
            if (commands[i].length > 1)
                commands[i][1] = this._rpfx(commands[i][1])
            commands[i].push(rpipe)
            this._redis_call(...commands[i])
        }

        let ret = await rpipe.exec()
        if (!Array.isArray(ret) || !ret.length) return Promise.resolve(ret)
        return Promise.resolve(ret.map((r) => r[1]))
    }

    /**
     * Calls Redis HMGET method, prefixing the key with the stored prefix.
     * If a callback function is provided, it is called with the result of the Redis method,
     * and the result of the callback function is returned as the Promise result.
     * If to_num is true, the result is parsed to number.
     * @async
     * @param {string} hkey - the key to access
     * @param {Array<string>} keys - the keys to retrieve
     * @param {boolean} [to_num=false] - whether to parse the result to number
     * @param {function} [cb] - the callback function to call with the result
     * @returns {Promise<Object>} the result of the Redis method, or a Promise resolved with the result
     */
    rhmget(hkey, keys, to_num, cb) {
        if (cb && typeof cb !== 'function') cb = null
        return this.rr('hmget', hkey, keys, (res) => {
            let out = {}
            for (let i = 0; i < keys.length; ++i) {
                out[keys[i]] = cb ? cb(res[i]) : to_num ? res[i] - 0 : res[i]
            }
            return out
        })
    }

    /**
     * Calls Redis RENAME method, prefixing the key with the stored prefix.
     * @async
     * @param {string} from - the key to rename
     * @param {string} to - the new key name
     * @returns {Promise<string>} the result of the Redis method, or a Promise resolved with the result
     */
    rrename(from, to) {
        return this._redis_call('rename', this._rpfx(from), this._rpfx(to))
    }

    /**
     * Deletes all keys from the given set, using either SPop or ZPopMin depending on the set type.
     * If a pattern is given, it is used to construct the key names to delete.     *
     * @param {string} keys_set - the set key name
     * @param {boolean} is_sorted - whether the set is sorted
     * @param {string} [ptn] - the pattern to use for constructing key names
     * @returns {Promise<void>} the result of the Redis method, or a Promise resolved with the result
     */
    async rdel_from_set(keys_set, is_sorted, ptn) {
        let _keys
        const command = is_sorted ? 'zpopmin' : 'spop'
        while (true) {
            _keys = await this.rr(command, keys_set, 500)
            if (!_keys || !Array.isArray(_keys) || !_keys.length) break
            if (is_sorted) _keys = _keys.filter((x, y) => !(y % 2))
            await this.rpipemulti(
                _keys.map((_key) => [
                    'del',
                    ptn ? ptn.replace('*', _key) : _key,
                ])
            )
        }
    }

    /**
     * Deletes keys or hash keys matching the given patterns.
     * If a pattern contains '.', it is assumed to be a hash key name.
     * If a pattern does not contain '*', it is used directly.
     * If a pattern contains '*', it is scanned for using the `SCAN` command.
     * @see {@link rscan}
     * @async
     * @param {string|string[]} ptns - the patterns to match
     * @returns {Promise<boolean>} the result of the Redis method, or a Promise resolved with the result
     */
    async rdel(ptns) {
        if (!Array.isArray(ptns)) ptns = [ptns]
        let commands = []
        for (let ptn of ptns) {
            if (!ptn) continue
            if (ptn.indexOf('*') === -1) {
                ptn = ptn.split('.')
                commands.push(
                    ptn.length === 1 ? ['del', ptn[0]] : ['hdel', ...ptn]
                )
            } else {
                await this.rscan(
                    ptn,
                    (_keys) => {
                        commands = commands.concat(
                            _keys.map((_key) => ['del', _key])
                        )
                    },
                    null,
                    { cb_all: true, count: 1000 }
                )
            }
        }
        if (commands.length) await this.rpipemaybe(commands)
        return Promise.resolve(true)
    }

    /**
     * Scans for keys or hash keys matching the given pattern.
     * If a pattern contains '.', it is assumed to be a hash key name.
     * If a pattern does not contain '*', it is used directly.
     * If a pattern contains '*', it is scanned for using the `SCAN` command.
     * @async
     * @param {string|string[]} ptn - the pattern to match
     * @param {function} [cb] - the callback to call with the matching keys
     * @param {string} [hkey] - the hash key to scan, if different from the key name
     * @param {object} [opts] - additional options
     * @param {boolean} [opts.return] - whether to return the keys in an array, or the return value of the callback
     * @param {boolean} [opts.return_cursor] - whether to return the final cursor value
     * @param {boolean} [opts.one] - whether to stop after finding one matching key
     * @param {number} [opts.cursor] - the initial cursor value
     * @param {boolean} [opts.cb_all] - whether to call the callback with the entire array of matching keys
     * @param {number} [opts.count] - the number of keys to return each iteration
     * @returns {Promise<string|string[]>} the result of the Redis method, or a Promise resolved with the result
     */
    async rscan(ptn, cb, hkey, opts) {
        //opts: return, return_cursor, one, cursor, cb_all, count
        if (!opts) opts = {}
        let params = []
        if (hkey) {
            hkey = this._rpfx(hkey)
            params.push(hkey)
        } else if (ptn) {
            ptn = this._rpfx(ptn)
        }
        let cb_ret
        let keys = []

        let i = opts.cursor ? opts.cursor - 0 : 0
        if (typeof cb !== 'function') cb = false
        const func = hkey ? 'hscan' : 'scan'
        const num_params = func === 'scan' ? 1 : 2
        params.push(i)
        if (ptn) params.push('MATCH', ptn)
        if (opts.count) params.push('COUNT', opts.count)

        while (true) {
            let result = await new Promise((resolve) =>
                this.redisClient[func](...params, (_err, _result) => {
                    resolve(_result)
                })
            )

            if (!Array.isArray(result) || !result.length) break
            //if()
            i = parseInt(result[0])
            params[func === 'scan' ? 0 : 1] = i
            if (
                result.length > 1 &&
                Array.isArray(result[1]) &&
                result[1].length
            ) {
                if (cb) {
                    if (opts.cb_all) {
                        cb_ret = cb(result[1])
                        if (cb_ret instanceof Promise) await cb_ret
                    } else {
                        while (result[1].length) {
                            let params_cb = result[1].splice(-num_params)
                            cb_ret = cb(...params_cb)
                            if (cb_ret instanceof Promise) await cb_ret
                        }
                    }
                } else if (opts.return) {
                    keys.splice(keys.length, 0, ...result[1])
                }
            }
            if (!i || opts.one) break
        }
        return opts.return_cursor ? [i, keys] : keys
    }

    /**
     * Retrieves a value from a Redis JSON key.
     * If key doesn't exists, returns null.
     * If key exists, but path doesn't - returns []
     * If a callback function is provided, it is called with the result of the Redis method,
     * and the result of the callback function is returned as the Promise result.
     * If empty_array_null is true, and the result is an empty array, returns null
     * If keep_array is false, and the result is an array with one element, returns that element
     * @async
     * @param {string} hkey - the key to access
     * @param {string} path - the path to the value to retrieve
     * @param {function} [callback] - the callback function to call with the result
     * @param {...string|function} [rest_args] - additional arguments to pass to the callback, or function to call with the key and path
     * @returns {Promise<Object>} the result of the Redis method, or a Promise resolved with the result
     */
    async rjget(hkey, path, callback, ...rest_args) {
        path = this._rjpath(path)
        hkey = this._rpfx(hkey)
        let res = await this._rraw('JSON.GET', hkey, path)
        let opts = {
            rerun: false,
            keep_array: false,
            empty_array_null: false,
        }

        Object.keys(opts).forEach((_opt) => {
            let _indx = rest_args.indexOf(_opt)
            if (_indx !== -1) {
                opts[_opt] = true
                rest_args.splice(_indx, 1)
            }
        })

        if (res !== null) {
            if (typeof res === 'string') res = parseJson(res, res)
            if (opts.empty_array_null && Array.isArray(res) && !res.length)
                res = null
            if (res !== null) {
                if (!opts.keep_array && Array.isArray(res) && res.length === 1)
                    res = res[0]
                if (callback && typeof callback === 'function') {
                    res = callback(res)
                    if (res instanceof Promise) res = await res
                }
            }
        }
        if (res !== null || !rest_args.length) return Promise.resolve(res)

        let ret
        const _type = typeof rest_args[0]
        if (_type === 'function') {
            ret = rest_args[0](hkey, path)
        } else {
            return Promise.resolve(null)
        }
        if (opts.rerun) {
            await ret
            return this.rjget(hkey, path, callback)
        } else {
            return ret
        }
    }

    /**
     * Retrieves an array of members from a Redis ZSET key.
     * Only members that have a score are included in the result.
     * If key doesn't exists, returns []
     * @async
     * @param {string} rkey - the key to access
     * @param {Array<string>} members_to_check - the members to check
     * @returns {Promise<Array<string>>} the result of the Redis method, or a Promise resolved with the result
     */
    async rinzset(rkey, members_to_check) {
        const res = await this.rr('zmscore', rkey, ...members_to_check)
        if (!res || !res.length) return Promise.resolve([])
        let out = []
        for (let i = 0; i < members_to_check.length; ++i) {
            if (res[i] !== null) out.push(members_to_check[i])
        }
        return Promise.resolve(out)
    }

    /**
     * Copies all members from a Redis ZSET key to a Redis SET key.
     * The copy is done in chunks using the ZSCAN method, so it won't block Redis
     * for long periods of time.
     * @async
     * @param {string} source_key - the ZSET key to copy from
     * @param {string} target_key - the SET key to copy to
     * @returns {Promise<void>} the result of the Redis method, or a Promise resolved with the result
     */
    async zset2set_scan(source_key, target_key) {
        //slow
        let cursor = '0'
        do {
            const [new_cursor, members] = await this.rr(
                'zscan',
                source_key,
                cursor
            )
            if (members.length > 0)
                await this.rr('sadd', target_key, ...members)
            cursor = new_cursor
        } while (cursor !== '0')
    }

    /**
     * Copies all members from a Redis ZSET key to a Redis SET or LIST key.
     * The copy is done in chunks using the ZRANGE method, so it won't block Redis
     * for long periods of time.
     * @async
     * @param {string} source_key - the ZSET key to copy from
     * @param {string} target_key - the LIST or SET key to copy to
     * @param {string} [command='sadd'] - the Redis command to use to add the members to the
     * target key, lpush for LIST, sadd for SET
     * @param {number} [limit=1000] - the number of members to copy at once
     * @param {boolean} [with_scores=false] - if true, every member is converted to a string with a comma separating the member value and its score
     * @returns {Promise<void>} the result of the Redis method, or a Promise resolved with the result
     */
    async zset_convert(source_key, target_key, command, limit, with_scores) {
        //command: to list - lpush, to set - sadd
        if (!command || command !== 'lpush') command = 'sadd'
        if (!limit) limit = 1000
        let start = 0
        while (true) {
            let members = []
            const _end = start + limit
            let params = ['zrange', source_key, start, _end]
            if (with_scores) params.push('WITHSCORES')
            const _members = await this.rr(...params)
            if (!_members || !_members.length) break
            if (with_scores) {
                for (let i = 0; i < _members.length; i += 2) {
                    members.push(_members[i] + ',' + _members[i + 1])
                }
            } else {
                members = _members
            }
            await this.rr(command, target_key, ...members)
            start = _end + 1
        }
    }

    _redis_call(redis_method, hkey, ...rest_args) {
        if (redis_method.indexOf('.') !== -1) {
            rest_args.unshift(hkey)
            return this._rraw(redis_method, ...rest_args)
        } else {
            if (
                rest_args.length > 0 &&
                hkey.substring(0, 1) === '{' &&
                (redis_method === 'del' ||
                    (redis_method.substring(0, 1) === 'z' &&
                        redis_method.indexOf('store') !== -1))
            )
                rest_args = rest_args.map((rest_arg) =>
                    typeof rest_arg !== 'string' ||
                    rest_arg.substring(0, 1) !== '{'
                        ? rest_arg
                        : this._rpfx(rest_arg)
                )

            const execer = this._rexec(rest_args)
            return execer[redis_method](hkey, ...rest_args)
        }
    }

    _rraw(redis_method, ...rest_args) {
        const execer = this._rexec(rest_args)
        if (
            redis_method.indexOf('JSON.') === 0 &&
            rest_args.length >= 3 &&
            (rest_args[2] !== null ||
                redis_method === 'JSON.SET' ||
                redis_method === 'JSON.ARRINSERT' ||
                redis_method === 'JSON.ARRAPPEND') &&
            typeof rest_args[2] !== 'function'
        )
            rest_args[2] = JSON.stringify(rest_args[2])
        if (redis_method === 'JSON.ARRINSERT' && rest_args.length >= 4)
            [rest_args[3], rest_args[2]] = [rest_args[2], rest_args[3]]
        return execer.call(redis_method, ...rest_args)
    }

    _rexec(rest_args) {
        if (
            rest_args &&
            rest_args[rest_args.length - 1] &&
            rest_args[rest_args.length - 1].exec
        )
            return rest_args.pop()
        return this.redisClient
    }

    _rjpath(path) {
        if (!path || path === '$') return '$'
        if (path.substr(0, 2) !== '$.') return '$.' + path
        return path
    }

    _rpfx(hkey) {
        let _slot_pfx = ''
        if (hkey.substring(0, 1) === '{') {
            _slot_pfx = '{'
            hkey = hkey.substring(1)
        }
        return (
            _slot_pfx +
            (hkey.indexOf(this.redisHprefix) !== 0 ? this.redisHprefix : '') +
            hkey
        )
    }
}

module.exports = new RedisUtilFunctions()
