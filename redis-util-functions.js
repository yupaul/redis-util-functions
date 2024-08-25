const Redis = require('ioredis')

const parseJson = (s, dflt) => {
	let out
	try {
		out = JSON.parse(s)
	} catch (_exc) {
		out = dflt
	}
	return out
}

class RedisUtilFunctions {

	constructor() {
		this.redisClient = null		
		this.redisHprefix = process.env.REDIS_HPREFIX ? process.env.REDIS_HPREFIX : false
	}

	rpipemaybe(commands) {		
		return (
			commands.length === 1 ?
			this.rr(...commands[0]) :
			this.rpipemulti(commands)			
		)		
	}

	rpipemulti(commands, arg2, cb) { //arg2 - 't' for transaction, function for callback
//let dt = Date.now() //-tmp	
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
			if (commands[i].length > 1) commands[i][1] = this._rpfx(commands[i][1])
			commands[i].push(rpipe)
			this._redis_call(...commands[i])
		}

		let ret = cb ? rpipe.exec((err, results) => cb(err, results)) : rpipe.exec()
		return ret
	}

	async rpipemulti2array(commands) {
		let rpipe = this.redisClient.pipeline()
		for (let i = 0; i < commands.length; ++i) {
			if (commands[i].length > 1) commands[i][1] = this._rpfx(commands[i][1])
			commands[i].push(rpipe)
			this._redis_call(...commands[i])
		}

		let ret = await rpipe.exec()
		if(!Array.isArray(ret) || !ret.length) return Promise.resolve(ret)
		return Promise.resolve(ret.map(r => r[1]))
	}

	_rexec(rest_args) {
		if (rest_args && rest_args[rest_args.length - 1] && rest_args[rest_args.length - 1].exec) return rest_args.pop()
		return this.redisClient
	}

	_redis_call(redis_method, hkey, ...rest_args) {
		if (redis_method.indexOf('.') !== -1) {
			rest_args.unshift(hkey)
			return this._rraw(redis_method, ...rest_args)
		} else {
			if(rest_args.length > 0 && hkey.substring(0, 1) === '{' && (redis_method === 'del' || (redis_method.substring(0, 1) === 'z' && redis_method.indexOf('store') !== -1))) rest_args = rest_args.map(rest_arg => (typeof rest_arg !== 'string' || rest_arg.substring(0, 1) !== '{') ? rest_arg : this._rpfx(rest_arg))
			
			const execer = this._rexec(rest_args)
			return execer[redis_method](hkey, ...rest_args)
		}
	}

	_rraw(redis_method, ...rest_args) {
		const execer = this._rexec(rest_args)
		if (redis_method.indexOf('JSON.') === 0 && rest_args.length >= 3 && (rest_args[2] !== null || redis_method === 'JSON.SET' || redis_method === 'JSON.ARRINSERT' || redis_method === 'JSON.ARRAPPEND') && typeof rest_args[2] !== 'function') rest_args[2] = JSON.stringify(rest_args[2])
		if(redis_method === 'JSON.ARRINSERT' && rest_args.length >= 4) [rest_args[3], rest_args[2]] = [rest_args[2], rest_args[3]]
		return execer.call(redis_method, ...rest_args)
	}

	r(redis_method, hkey, ...rest_args) {
		hkey = this._rpfx(hkey)
		if (!rest_args.length || typeof rest_args[rest_args.length - 1] !== 'function') rest_args.push(function() {})
		return this._redis_call(redis_method, hkey, ...rest_args)
	}

	rr(redis_method, hkey, ...rest_args) {
		hkey = this._rpfx(hkey)
		let cb = rest_args.length > 0 && typeof rest_args[rest_args.length - 1] === 'function' ? rest_args.pop() : null
		if (!cb) return this._redis_call(redis_method, hkey, ...rest_args)

		return new Promise(async (resolve, reject) => {
			let f = (err, result) => {
				if (err) {
					logger.ERROR.error(logger.lineNumber(new Error(), 2) + ' | ' + err)
					return reject(err)
				}
				return resolve(cb(result))
			}
			rest_args.push(f)
			this._redis_call(redis_method, hkey, ...rest_args)
		})
	}

	rhmget(hkey, keys, to_num, cb) {
		if(cb && typeof cb !== 'function') cb = null
		return this.rr('hmget', hkey, keys, (res) => {
			let out = {}
			for(let i = 0; i < keys.length; ++i) {
				out[keys[i]] = cb ? cb(res[i]) : (to_num ? res[i] - 0 : res[i])
			}
			return out
		})
	}
	
	rrename(from, to) {
		return this._redis_call('rename', this._rpfx(from), this._rpfx(to))
	}

	async rdel_from_set(keys_set, is_sorted, ptn) {
		let _keys
		const command = is_sorted ? 'zpopmin' : 'spop'
		while(true) {			
			_keys = await this.rr(command, keys_set, 500)
			if(!_keys || !Array.isArray(_keys) || !_keys.length) break
			if(is_sorted) _keys = _keys.filter((x, y) => !(y % 2))
			await this.rpipemulti(_keys.map(_key => ['del', ptn ? ptn.replace('*', _key) : _key]))
		}
	}

	async rdel(ptns) {	
		if(!Array.isArray(ptns)) ptns = [ptns]
		let commands = []
		for(let ptn of ptns) {		
			if (!ptn) continue
			if (ptn.indexOf('*') === -1) {
				ptn = ptn.split('.')
				commands.push(ptn.length === 1 ? ['del', ptn[0]] : ['hdel', ...ptn])
			} else {
				await this.rscan(ptn, _keys => {
					commands = commands.concat(_keys.map(_key => ['del', _key]))
				}, null, {cb_all : true, count: 1000})		
			}			
		}
		if(commands.length) await this.rpipemaybe(commands)
		return Promise.resolve(true)
	}

	async rscan(ptn, cb, hkey, opts) {
		//opts: return, return_cursor, one, cursor, cb_all, count
		if(!opts) opts = {}
		let params = []
		if(hkey) {
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
		if(ptn) params.push('MATCH', ptn)
		if(opts.count) params.push('COUNT', opts.count)

		while (true) {
			let result = await new Promise((resolve) => this.redisClient[func](...params, (_err, _result) => {
				resolve(_result)
			}))

			if (!Array.isArray(result) || !result.length) break
			//if()
			i = parseInt(result[0])
			params[func === 'scan' ? 0 : 1] = i
			if (result.length > 1 && Array.isArray(result[1]) && result[1].length) {
				if (cb) {
					if(opts.cb_all) {
						cb_ret = cb(result[1])
						if(cb_ret instanceof Promise) await cb_ret
					} else {					
						while(result[1].length) {
							let params_cb = result[1].splice(-num_params)
							cb_ret = cb(...params_cb)
							if(cb_ret instanceof Promise) await cb_ret
						}
					}
				} else if (opts.return) {
					keys.splice(keys.length, 0, ...result[1])
				}
			}
			if (!i || opts.one) break
		}
		return (opts.return_cursor ? [i, keys] : keys)
	}

	open() {
		if (parseInt(process.env.REDIS_CLUSTER)) {
			this.redisClient = new Redis.Cluster([process.env.REDIS_CONNECTION])
		} else {
			this.redisClient = new Redis(process.env.REDIS_CONNECTION)
		}
	}


	async rjget(hkey, path, callback, ...rest_args) {
		// if key doesn't exists return null
		// if key exists, but path doesn't - returns []
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
			if(_indx !== -1) {
				opts[_opt] = true
				rest_args.splice(_indx, 1)				
			}			
		})
		
		if (res !== null) {
			if (typeof res === 'string') res = parseJson(res, res)
			if (opts.empty_array_null && Array.isArray(res) && !res.length) res = null
			if (res !== null) {
				if (!opts.keep_array && Array.isArray(res) && res.length === 1) res = res[0]
				if (callback && typeof callback === 'function') {
					res = callback(res)
					if (res instanceof Promise) res = await res
				}
			}
		}
		if (res !== null || !rest_args.length) return Promise.resolve(res)

		let ret
		const _type = typeof rest_args[0]
		if (_type === 'string') {
			ret = this.qry(...rest_args)
		} else if (_type === 'function') {
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

	async rjset(hkey, path, data, ...rest_args) {
		//hkey path [db_query] [db_query_params, Array or function] [callback]
		const db_args = rest_args.length && typeof rest_args[0] === 'string' ? rest_args.splice(0, 2) : false
		path = this._rjpath(path)
		if (db_args) {
			await this.rr('JSON.SET', hkey, path, data, ...rest_args)
			return this.qry(db_args[0], db_args.length > 1 ? (typeof db_args[1] === 'function' ? db_args[1](data) : db_args[1]) : data)
		} else {
			return this.rr('JSON.SET', hkey, path, data, ...rest_args)
		}
	}

	rjset_sync(hkey, path, data, ...rest_args) {
		//hkey path [db_query] [db_query_params, Array or function] [callback]
		const db_args = rest_args.length && typeof rest_args[0] === 'string' ? rest_args.splice(0, 2) : false
		path = this._rjpath(path)
		this.r('JSON.SET', hkey, path, data, ...rest_args)
		if (db_args) this.pool.query(db_args[0], db_args.length > 1 ? (typeof db_args[1] === 'function' ? db_args[1](data) : db_args[1]) : data)
	}

	_rjpath(path) {
		if (!path || path === '$') return '$'
		if (path.substr(0, 2) !== '$.') return '$.' + path
		return path
	}

	_rpfx(hkey) {
		let _slot_pfx = ''
		if(hkey.substring(0, 1) === '{') {
			_slot_pfx = '{'
			hkey = hkey.substring(1)
		}
		return _slot_pfx + (hkey.indexOf(this.redisHprefix) !== 0 ? this.redisHprefix : '') + hkey
	}


	async rinzset(rkey, members_to_check) {
		const res = await this.rr('zmscore', rkey, ...members_to_check)
		if(!res || !res.length) return Promise.resolve([])
		let out = []
		for(let i = 0; i < members_to_check.length; ++i) {
			if(res[i] !== null) out.push(members_to_check[i])
		}
		return Promise.resolve(out)
	}	


	async zset2set_scan(source_key, target_key) {
		//slow
		let cursor = '0'
		do {
		  const [new_cursor, members] = await this.rr('zscan', source_key, cursor)
		  if (members.length > 0) await this.rr('sadd', target_key, ...members)		  
		  cursor = new_cursor
		} while (cursor !== '0')
	}	

	async zset_convert(source_key, target_key, command, limit, with_scores) {
		//command: to list - lpush, to set - sadd
		if(!command || command !== 'lpush') command = 'sadd'
		if(!limit) limit = 1000
		let start = 0
		while(true) {
		  let members = []
		  const _end = start + limit
		  let params = ['zrange', source_key, start, _end]
		  if(with_scores) params.push('WITHSCORES')
		  const _members = await this.rr(...params)
		  if (!_members || !_members.length) break
		  if(with_scores) {
            for(let i = 0; i < _members.length; i += 2) {
                members.push(_members[i]+','+_members[i+1])
            }    
		  } else {
			members = _members
		  }
		  await this.rr(command, target_key, ...members)		  
		  start = _end + 1
		}
	}


}

module.exports = new RedisUtilFunctions()
