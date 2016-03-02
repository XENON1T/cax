import config

def copy(f1, f2,
         server,
         username):
    util.log_to_file('ssh.log')
    ssh = SSHClient()
    ssh.load_system_host_keys()

    ssh.connect(server,
                username=username)


    # SCPCLient takes a paramiko transport as its only argument
    scp = SCPClient(ssh.get_transport())

    scp.put(f1, f2,
            recursive=True)

    scp.close()

def clear():
    # Grab the Run DB so we can query it
    collection = config.mongo_collection()

    # For each TPC run, check if should be uploaded
    for doc in collection.find({'detector' : 'tpc'}):
        here = None
        copies = []

        if 'data' not in doc:
            continue
        
        # Iterate over data locations to know status
        for datum in doc['data']:
            # Is host known?
            if 'host' not in datum:
                continue

            if datum['status'] != 'transferred':
                continue

            # If the location refers to here
            if datum['host'] == config.get_hostname():
                # Was data transferred here?
                if datum['status'] == 'transferred':
                    # If so, store info on it.
                    here = datum
            else:
                copies.append(datum)

        print(doc['name'], len(copies))
#        print(here, copies)



if __name__ == "__main__":
    clear()
