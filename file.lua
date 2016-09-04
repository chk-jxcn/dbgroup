-- map record to file

require "config"
require "lfs"
local _M = {}
local filesonedir = config.storage.filesonedir or 100
local recordsonefile = config.storage.recordsonefile or 1000
local recordsonedir = filesonedir * recordsonefile
local datapath = config.storage.datapath or "./data"

function _M.todir(record)
	local dirname = tostring(math.floor(record/recordsonedir))
	return datapath .. "/" .. dirname
end

function _M.tofile(record)
	local dirname = tostring(math.floor(record/recordsonedir))
	local filename = string.sub(record, #dirname + 1)
	return _M.todir(record) .. "/" .. filename
end

function _M.preparedir(record)
	local dir = _M.todir(record)
	local dirmode = lfs.attributes(dir, "mode")
	if not dirmode then
		lfs.mkdir(dir)
		return
	elseif dirmode ~= "directory" then
		error(dir .. "exist")
	end
end
return _M

