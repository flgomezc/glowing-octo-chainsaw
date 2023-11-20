import argparse, sys, subprocess
from datetime import date
from pprint import pprint
from rucio.client import Client
client = Client()

SCOPE = "cms"
OPERATOR = 'fgomezco'
DATE = str(date.today())

REASON = f"last replica corrupted operation perfomed by {OPERATOR} on {DATE}"

def get_stuck_locks_for_rule(ruleid):
    """
    Gets the rule info, then the stuck locks and tests the last available 
    replica. If the last replica is corrupted, then the file is RUCIO 
    invalidated. 
    """
    #ruleinfo = client.get_replication_rule(ruleid)
    rule_locks = list( client.list_replica_locks(ruleid) )
    stuck_locks_list = []
    for lock in rule_locks:
        if lock['state'] != 'OK' :
            #print(lock)
            stuck_locks_list.append(lock)
    for lock in stuck_locks_list:
        test_if_it_is_last_file_replica(lock)
    return None

def test_if_it_is_last_file_replica(lock):
    """Check and count AVAILABLE and UNAVAILABLE replicas"""
    filename = lock['name']
    file_did = {"scope":SCOPE, "name":filename}

    file_replicas_all_states = list(client.list_replicas( 
        [file_did], all_states=True ))
    rucio_adler32 = file_replicas_all_states[0]['adler32']
    replicas_rses = list(file_replicas_all_states[0]["states"].keys())

    counter_availables = 0
    pfn_sources_to_test = []
    last_replica_rse = None

    for rse in replicas_rses:
        if file_replicas_all_states[0]["states"][rse] == "AVAILABLE":
            counter_availables += 1
            last_replica_rse = rse
            pfn_sources_to_test.append( 
                file_replicas_all_states[0]["rses"][rse] )

    if counter_availables == 1: # Yes, it is a last replica!
        pfn = pfn_sources_to_test[0][0]
        # DO GFAL_COPY, gfal-sum ADLER32 and Rucio DeclareBadReplica
        is_corrupted_replica(pfn, rucio_adler32, filename, last_replica_rse)
    return 0

def is_corrupted_replica(pfn, rucio_adler32, filename, last_replica_rse):
    """
    Test if the last replica is corrupted:
       * Do the gfal-copy to local, 
       * gfal-sum basename adler32
       * compare local_adler32 with rucio_adler32 and declare bad replica
         if necessary.
    """
    basename = filename.split('/')[-1] #get the last part of  blaa.root
    file_did = {"scope":SCOPE, "name":filename}

    subprocess.call(['gfal-copy', pfn , '/tmp/'])
    r = subprocess.run(['gfal-sum', '/tmp/'+basename , 'adler32'], 
                       stdout= subprocess.PIPE, stderr = subprocess.PIPE,
                       check=False)
    if r.returncode != 0:
        print(f"{filename},{last_replica_rse}, ERROR failed invalidation")
        # TODO
        return None
    result = r.stdout.decode('UTF-8').splitlines()[0].split()
    local_adler32 = result[1]

    if local_adler32 != rucio_adler32:
    # Candidate elegible for Last Replica Corrupted
        declare = client.declare_bad_did_replicas(rse=last_replica_rse,
                                              dids=[file_did], reason=REASON)
        print(f"{filename},{last_replica_rse},SUCCESS file INVALIDATED,{declare}")

    return None

def read_rules_from_file(list_of_rules):
    """
    Open the list of files specified on fi
    """
    with open(list_of_rules, 'r', encoding='utf-8') as f:
        rule_list = f.readlines()
        for rule in rule_list:
            rule = rule.strip()
    return rule_list

def main():
    """Main function"""
    parser = argparse.ArgumentParser()

    validgroup = parser.add_mutually_exclusive_group(required=True)
    validgroup.add_argument("--single-rule", type=str,
                help="enter the single Rucio rule ID to check")
    validgroup.add_argument("--list-of-rules", type=str,
                help="enter the .txt filename to check (one rule per line)")
    
    parser.parse_args(args=None if sys.argv[1:] else ['--help'])
    try:
        args = parser.parse_args()
    except ValueError:
        parser.print_help()
        sys.exit(0)

    if args.single_rule:
        rule_list = [args.single_rule]
    elif args.list_of_rules:
        rule_list = read_rules_from_file(args.list_of_rules)

    for ruleid in rule_list:
        get_stuck_locks_for_rule(ruleid)
    return 0

if __name__ == "__main__":
    main()
