config = {
	idstart = 1000000,
	idend = 90000000,
	threadnum = 20,
	storage = {
		recordsonefile = 1000,
		filesonedir = 100,
		datapath = "./data"
	},

	urlconfig = {
		url = [[https://www.douban.com/group/topic/$id/]],
		pat = "%$(%w+)"
	}
}

