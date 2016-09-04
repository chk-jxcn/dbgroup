require "task"
require "lfs"
local fileop = require "file"
require "config"

local idstart = config.idstart
local idend = config.idend
local topics = {}
local threads = {}
local datapath = config.storage.datapath

for i = idstart, idend, config.storage.recordsonefile do
	topics[i] = 0
end

local function loadrecord()
	for dir in lfs.dir(datapath) do
		if dir ~= "." and dir ~= ".." then
			local dirpath = datapath .. "/" .. dir
			for file in lfs.dir(dirpath) do
				if file ~= "." and file ~= ".." then				
					topics[tonumber(dir .. file)] = 1
				end
			end
		end
	end
end

local function createthreads()
	local me = task.id()
	for i = 1, config.threadnum do
		local taskid = task.create("worker.lua", {me})
		threads[taskid] = "start"
	end
end

local function processmsg()
	local msg, id
	repeat 
		msg, id = task.receive(-1)
		-- XXX: 线程中断后分发该任务到其他线程
		-- 线程数一直在减少，直到服务器不再返回错误数据
		if string.match(msg, "^stop") then
			print("!!!thread " .. id .. " end. " .. msg)
			threads[id] = nil
			if table.maxn(threads) == 0 then
				return false, "all threads exit"
			end
		else
			break
		end
	until nil
	if tonumber(msg) then
		print(msg .. " complete " .. "by " .. id)
	end
	return id
end


local function dispatch()
	for i = idstart, idend, config.storage.recordsonefile do
		k = i
		v = topics[i]
		if v == 0 then
			local ret, err = processmsg()
			if not ret then
				return false, err
			end
			fileop.preparedir(tostring(k))
			task.post(ret, tostring(k), 0)
		end
	end
	repeat
		local ret, err = processmsg()
		if not ret then
			break
		else
			task.post(ret, "stop")
		end
	until nil
	return true
end

local function stop()
	if table.maxn(threads) == 0 then return end
	for k, v in pairs(threads) do
		task.post(k, "stop", 0)
	end
	repeat 
		if table.maxn(threads) == 0 then return end
		msg, id = task.receive(-1)
		print(msg)
		threads[id] = nil
	until nil
end
		

loadrecord()
createthreads()

ret,err =  pcall(dispatch)
print(err)

if not ret then
	stop()
end
