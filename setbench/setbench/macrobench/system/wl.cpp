#include "global.h"
#include "helper.h"
#include "wl.h"
#include "row.h"
#include "table.h"
#include "all_indexes.h"
#include "catalog.h"
#include "mem_alloc.h"
#include <experimental/filesystem>

RC workload::init() {
	sim_done = false;
	return RCOK;
}

void workload::setbench_deinit() {

}

RC workload::init_schema(std::string schema_file) {

//    RLU_INIT(RLU_TYPE_FINE_GRAINED, 1);
//    rlu_self = &rlu_tdata[tid];
//    RLU_THREAD_INIT(rlu_self);

    assert(sizeof(uint64_t) == 8);
    assert(sizeof(double) == 8);
	string line;

    if (!std::experimental::filesystem::exists(schema_file)) {
        std::cerr<<std::endl;
        std::cerr<<"#######################################################################"<<std::endl;
        std::cerr<<"#### ERROR: file "<<schema_file<<" not found."<<std::endl;
        std::cerr<<"####        You are probably running with the wrong working directory."<<std::endl;
        std::cerr<<"####        This benchmark must be run from directory: macrobench/."<<std::endl;
        std::cerr<<"#######################################################################"<<std::endl;
        std::cerr<<std::endl;
        exit(-1);
    }

    if (g_synth_table_size % g_init_parallelism) {
        std::cerr<<std::endl;
        std::cerr<<"################################################################################"<<std::endl;
        std::cerr<<"#### ERROR: init size="<<g_synth_table_size<<" is not divisible by nthreads="<<g_init_parallelism<<std::endl;
        std::cerr<<"####        however, macrobench requires this to be true!"<<std::endl;
        std::cerr<<"####        (This is an idiosyncrasy of DBx1000 that we haven't fixed...)"<<std::endl;
        std::cerr<<"################################################################################"<<std::endl;
        std::cerr<<std::endl;
        exit(-1);
    }

	ifstream fin(schema_file);
    Catalog * schema;
    int indexesCreated = 0;
    while (getline(fin, line)) {
        if (line.compare(0, 6, "TABLE=")==0) {
            string tname;
            tname = &line[6];
            schema = (Catalog *) _mm_malloc(sizeof (Catalog), CL_SIZE);
            getline(fin, line);
            int col_count = 0;
            // Read all fields for this table.
            std::vector<string> lines;
            while (line.length()>1) {
                lines.push_back(line);
                getline(fin, line);
            }
            schema->init(tname.c_str(), lines.size());
            for (UInt32 i = 0; i<lines.size(); i++) {
                string line = lines[i];
                size_t pos = 0;
                string token;
                int elem_num = 0;
                int size = 0;
                string type;
                string name;
                while (line.length()!=0) {
                    pos = line.find(",");
                    if (pos==string::npos)
                        pos = line.length();
                    token = line.substr(0, pos);
                    line.erase(0, pos+1);
                    switch (elem_num) {
                        case 0: size = atoi(token.c_str());
                            break;
                        case 1: type = token;
                            break;
                        case 2: name = token;
                            break;
                        default: assert(false);
                    }
                    elem_num++;
                }
                assert(elem_num==3);
                schema->add_col((char *) name.c_str(), size, (char *) type.c_str());
                col_count++;
            }
            table_t * cur_tab = (table_t *) _mm_malloc(sizeof (table_t), CL_SIZE);
            cur_tab->init(schema);
            tables[tname] = cur_tab;
        } else if (!line.compare(0, 6, "Index=")) {
            string iname;
            iname = &line[6];
            getline(fin, line);

            std::vector<string> items;
            string token;
            size_t pos;
            while (line.length()!=0) {
                pos = line.find(",");
                if (pos==string::npos)
                    pos = line.length();
                token = line.substr(0, pos);
                items.push_back(token);
                line.erase(0, pos+1);
            }


            string tname(items[0]);
//            cout<<"tname="<<tname<<" iname="<<iname<<" sizeof(Index)="<<sizeof(Index)<<std::endl;
            Index * index = (Index *) _mm_malloc(sizeof (Index), ALIGNMENT);
            new(index) Index();
            int part_cnt = (CENTRAL_INDEX) ? 1 : g_part_cnt;
            if (tname=="ITEM")
                part_cnt = 1;
#ifdef IDX_HASH
#   if WORKLOAD == YCSB
            index->init(part_cnt, tables[tname], g_synth_table_size*2);
#   elif WORKLOAD == TPCC
            assert(tables[tname]!=NULL);
            index->init(part_cnt, tables[tname], stoi(items[1])*part_cnt);
#   endif
#else
            index->init(part_cnt, tables[tname]);
#endif
            indexes[iname] = index;
            index->index_id = indexesCreated;
            index->index_name = iname;
            indexesCreated++;
        }
    }
    fin.close();

//    RLU_THREAD_FINISH(rlu_self);
//    RLU_FINISH();

    return RCOK;
}



void workload::index_insert(std::string index_name, uint64_t key, row_t * row) {
	assert(false);
	Index * index = (Index *) indexes[index_name];
	index_insert(index, key, row);
}

void workload::index_insert(Index * index, uint64_t key, row_t * row, int64_t part_id) {
	uint64_t pid = part_id;
	if (part_id == -1)
		pid = get_part_id(row);
	itemid_t * m_item =
		(itemid_t *) mem_allocator.alloc( sizeof(itemid_t), pid );
	m_item->init();
	m_item->type = DT_row;
	m_item->location = row;
	m_item->valid = true;

    RC result = index->index_insert(key, m_item, pid);
    assert(result == RCOK);
}

void workload::initThread(const int __tid) {
    for (map<string,Index*>::iterator it = indexes.begin(); it!=indexes.end(); it++) {
        it->second->initThread(__tid);
    }
}

void workload::deinitThread(const int __tid) {
    for (map<string,Index*>::iterator it = indexes.begin(); it!=indexes.end(); it++) {
        it->second->deinitThread(__tid);
    }
}
