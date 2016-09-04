require "task"
require "luacurl"
require "config"
require "mime"
local fileop = require "file"


local main = arg[1]
local me = task.id()

local function post(s)
	if me == 1 then
		print(s)
	else
		task.post(main, s, me)
	end
end

local function receive(time)
	if me == 1 then
		return  "1000000"
	else
		return task.receive(time)
	end
end

math.randomseed(os.time() + me * 100)
local function newbid()
	local bid = ""
	for i = 1,8 do bid = bid .. string.char(math.random(0,255)) end
	local bid = mime.b64(bid)
	return "bid=" .. string.sub(bid,1,-2)
end

local _M = {len = 0, str = ""}

function _M.resetbuff(t)
	t.len = 0
	t.str = ''
end

function _M.initcurl(t)
	t.c=curl.new()
	local c = t.c
	c:setopt(curl.OPT_WRITEDATA, t)
	c:setopt( curl.OPT_WRITEFUNCTION, function (t, buffer )
		t.len = t.len + string.len(buffer)
		t.str = t.str .. buffer
		return t.len
	end);
	-- c:setopt(curl.OPT_HTTPHEADER, "User-Agent: Chrome/52.0.2743.116")
	-- c:setopt(curl.OPT_HTTPHEADER, "Connection: close")
	return c
end

function _M.onerequest(t, req)
	t:resetbuff()
	local c = t.c
	local bid = newbid()
	local urlconfig = config.urlconfig
	c:setopt(curl.OPT_USERAGENT, "Chrome/52." .. math.random(0, 100) .."." .. math.random(1000,3000) .."." .. math.random(100,600))	
	c:setopt(curl.OPT_COOKIE, bid) 
	c:setopt(curl.OPT_URL, string.gsub(urlconfig.url, urlconfig.pat, req)) 
	c:perform()
	return c:getinfo(curl.INFO_RESPONSE_CODE)
end

local function errreq(t, req)
	local f = io.open(me .. "err", "w+")
	f:write(req.id .. "\n" .. t.str)
	f:close()
end

local lastest = 0

function _M.processcontent(t, code, req, cache)
	if code ~= "200" then
		if t.len < 5000 then
			-- Simply check length of content
			errreq(t, req)
			return false, "stop:content too short"
		end
	end
	-- Here we get a normal page
	local uid = string.match(t.str, [[https://www.douban.com/people/([^/]+)/]]) 
	if uid then
		table.insert(cache, {req.id, uid})
		lastest = req.id
	end
	-- print(req.id, uid)
	return true
end

local function writetofile(id, cache)
	local filename = fileop.tofile(id)
	local f = io.open(filename, "w+")
	for k,v in ipairs(cache) do
		f:write(v[1] .. " " .. v[2] .. "\n")
	end
	f:close()

end 

function _M.processcmd(t, cmd)
	local id = tonumber(cmd)
	local cache = {}
	for id = cmd, cmd+config.storage.recordsonefile - 1 do
		if receive(0) == "stop" then return false, "stop:stop by user" end
		local req = {id=id}
		local retcode = _M:onerequest(req)
		local ret, err = _M:processcontent(retcode, req, cache) 
		if not ret then
			return false, err
		end
	end
	writetofile(id, cache)
	return true
end

local function run ()
	_M:initcurl()
	post("start")
	repeat
		local cmd = receive(-1)
		if cmd == "stop" then
			post(cmd)
			break
		else
			local ret,err = _M:processcmd(cmd)
			if not ret then
				post(err)
				break
			else
				post(lastest)
			end
		end
	until nil
end
	
run()
	
