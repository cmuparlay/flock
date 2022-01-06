import sqlite3
import os.path
import datetime
from multiprocessing.dummy import Pool as ThreadPool
from subprocess import CalledProcessError
from subprocess import check_output

#--------------------------- DEFINITIONS -------------------------------#

DB_FILENAME = "results.db"
NUM_PROCESSORS = 4
PARALLEL_CHUNK_SIZE = 100

#table column as tuples of column name, type, string in the output and delimiter
columns = [
            ("step"                     ,"text"     ,"step"                             ,"=")
           ,("machine"                  ,"text"     ,"machine"                          ,"=")
           ,("trial"                    ,"integer"  ,"trial"                            ,"=")
           ,("binary"                   ,"text"     ,"binary"                           ,"=")
           ,("ds"                       ,"text"     ,"data_structure"                   ,"=")
           ,("find_func"                ,"text"     ,"FIND_FUNC"                        ,"=")
           ,("insert_func"              ,"text"     ,"INSERT_FUNC"                      ,"=")
           ,("erase_func"               ,"text"     ,"ERASE_FUNC"                       ,"=")
           ,("rq_func"                  ,"text"     ,"RQ_FUNC"                          ,"=")
           ,("reclaim"                  ,"text"     ,"RECLAIM"                          ,"=")
           ,("alloc"                    ,"text"     ,"ALLOC"                            ,"=")
           ,("pool"                     ,"text"     ,"POOL"                             ,"=")
           ,("prefill"                  ,"text"     ,"PREFILL"                          ,"=")
           ,("run_time"                 ,"integer"  ,"MILLIS_TO_RUN"                    ,"=")
           ,("ins"                      ,"integer"  ,"INS_FRAC"                              ,"=")
           ,("del"                      ,"integer"  ,"DEL_FRAC"                              ,"=")
           ,("rq"                       ,"integer"  ,"RQ"                               ,"=")
           ,("rq_size"                  ,"integer"  ,"RQSIZE"                           ,"=")
           ,("max_key"                  ,"integer"  ,"MAXKEY"                           ,"=")
           ,("work_th"                  ,"integer"  ,"WORK_THREADS"                     ,"=")
           ,("rq_th"                    ,"integer"  ,"RQ_THREADS"                       ,"=")
           ,("total_find"               ,"integer"  ,"total find"                       ,":")
           ,("total_rq"                 ,"integer"  ,"total rq"                         ,":")
           ,("total_update"             ,"integer"  ,"total update"                     ,":")
           ,("total_queries"            ,"integer"  ,"total queries"                    ,":")
           ,("total"                    ,"integer"  ,"total ops"                        ,":")
           ,("find_throughput"          ,"integer"  ,"find throughput"                  ,":")
           ,("rq_throughput"            ,"integer"  ,"rq throughput"                    ,":")
           ,("query_throughput"         ,"integer"  ,"query throughput"                 ,":")
           ,("update_throughput"        ,"integer"  ,"update throughput"                ,":")
           ,("throughput"               ,"integer"  ,"total throughput"                 ,":")
           ,("elapsed_time"             ,"integer"  ,"elapsed milliseconds"             ,":")
           ,("napping_overtime"         ,"integer"  ,"napping milliseconds overtime"    ,":")
           ,("avg_latency_updates"      ,"integer"  ,"average latency_updates total"    ,"=")
           ,("stdev_latency_updates"    ,"integer"  ,"stdev latency_updates total"      ,"=")
           ,("min_latency_updates"      ,"integer"  ,"min latency_updates total"        ,"=")
           ,("max_latency_updates"      ,"integer"  ,"max latency_updates total"        ,"=")
           ,("avg_latency_searches"     ,"integer"  ,"average latency_searches total"   ,"=")
           ,("stdev_latency_searches"   ,"integer"  ,"stdev latency_searches total"     ,"=")
           ,("min_latency_searches"     ,"integer"  ,"min latency_searches total"       ,"=")
           ,("max_latency_searches"     ,"integer"  ,"max latency_searches total"       ,"=")
           ,("avg_latency_rqs"          ,"integer"  ,"average latency_rqs total"        ,"=")
           ,("stdev_latency_rqs"        ,"integer"  ,"stdev latency_rqs total"          ,"=")
           ,("min_latency_rqs"          ,"integer"  ,"min latency_rqs total"            ,"=")
           ,("max_latency_rqs"          ,"integer"  ,"max latency_rqs total"            ,"=")
]

ignore_warnings_on = set([""])

def process_chunk(start_ix):
    global files
    global ignore_warnings_on
    global columns
    global PARALLEL_CHUNK_SIZE

    #populate the table by going over all files
    warnings = []
    insertion_strings = []
    k = 0
    print start_ix
    for file_name in files:
        if k < start_ix or k >= start_ix+PARALLEL_CHUNK_SIZE:
            k += 1
            continue

        try:
            res = check_output('cat ' + file_name + ' | grep "Validation OK" | wc -l', shell=True)
        except CalledProcessError, e:
            print e
            exit()
        if "0" in res:
            print "ERORR: file " +file_name+ " had a validation error"
            continue #skip files with segfaults or errors

#        if ((k % 100) == 0):
#            print k,
            #print "processing file #" + repr(k) + " of " + repr(len(files))
            #print datetime.datetime.now()
        insert_string = "INSERT INTO results VALUES ("

        #get all needed values from the file
        first = True
        for (col,col_type,col_string,delim) in columns:
            if delim == "=":
                cmd = 'file_name="'+file_name+'"; key="'+col_string+delim+'" ; cat $file_name | grep "$key" | cut -d"'+delim+'" -f2 | tail -1'
            else:
                cmd = 'file_name="'+file_name+'"; key="'+col_string+'" ; cat $file_name | grep "$key" | cut -d"'+delim+'" -f2 | tail -1'
            try:
                res = check_output(cmd, shell=True)
            except CalledProcessError, e:
                print e
                exit()

            if not res:
                if not col_string in ignore_warnings_on:
                    warnings.append((file_name, col, col_type, col_string, delim, res, insert_string))
                if col_type == "text":
                    res = ""
                else:
                    res = "0"

            if first:
                first = False
                prefix=''
            else:
                prefix=','
            if col_type == "text":
                insert_string += prefix+"'"+res.strip()+"'"
            else:
                insert_string += prefix+res.strip()

        insert_string += ")"
        insertion_strings.append(insert_string)

        k += 1

#    for w in warnings:
#        print "WARNING: failed to fetch " + repr(w[3]) + " from file " + repr(w[0]) #+ " response=" + repr(w[5])

    return insertion_strings

#--------------------------- DEFINITIONS END -------------------------------#

if os.path.isfile(DB_FILENAME):
    print "Database file '" + DB_FILENAME + "' already exists!"
    exit()

#print "Removing old database..."
##try to remove old database
#try:
#    res = check_output("rm " + DB_FILENAME, shell=True)
#except CalledProcessError, e:
#    print DB_FILENAME + " not found"

#create new database
print "Creating new database '" + DB_FILENAME + "'..."
conn = sqlite3.connect(DB_FILENAME)
cursor = conn.cursor()

#add the results table to the database
print "Creating results table..."
create_table_string = "CREATE TABLE results ("
first = True
for (col,col_type,string,delim) in columns:
    if first:
        first = False
        prefix=''
    else:
        prefix=','
    create_table_string += prefix+" "+col+" "+col_type
create_table_string+= ")"

# Create table
cursor.execute(create_table_string)
print "Table created."

#get all files
print "Fetching list of data files..."
try:
    res = check_output("ls data/step*", shell=True)
except CalledProcessError, e:
    print e
    exit()
files = res.strip().split("\n")

# use thread pool to process all files
print "Processing " + repr(len(files)) + " files..."
print "Creating thread pool with " + repr(NUM_PROCESSORS) + " threads... " + repr(datetime.datetime.now())
pool = ThreadPool(NUM_PROCESSORS)
startix = 0
endix = len(files)
results = pool.map(process_chunk, range(startix, endix, PARALLEL_CHUNK_SIZE))
print "Joined thread pool. " + repr(datetime.datetime.now())

print "Performing insertions..."
k = 0
for flat_results in results:
    for insert_string in flat_results:
        if ((k%100) == 0): print "Insertion " + repr(k)
        try:
            cursor.execute(insert_string)
        except:
            print insert_string
        k += 1
print "Committing all processed data..."
conn.commit()
print "Commit finished."

conn.close()
