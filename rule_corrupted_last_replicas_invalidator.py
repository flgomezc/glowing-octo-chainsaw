import argparse, sys, subprocess
from datetime import date
from pprint import pprint
from rucio.client import Client
client = Client()

SCOPE = "cms"
OPERATOR = 'fgomezco'
DATE = str(date.today())

REASON_LAST_CORRUPTED = f"last replica corrupted operation perfomed by {OPERATOR} on {DATE}"
REASON_ALL_INVALID = f"all replicas ara UNAVAILABLE opreation performed by {OPERATOR} on {DATE}"

class StuckFile:
    def __init__(self, ruleid, lfn, rule_rse_expression):
        self.lfn = lfn
        self.ruleid = ruleid
        self.rule_rse_expression = rule_rse_expression
        return None

def get_stuck_locks_for_rule(ruleid):
    """
    Gets the rule info, then the stuck locks and tests the last available 
    replica. If the last replica is corrupted, then the file is RUCIO 
    invalidated. 
    """
    ruleinfo = client.get_replication_rule(ruleid)
    rule_rse_expression = ruleinfo['rse_expression']

    rule_locks = list( client.list_replica_locks(ruleid) )
    stuck_locks_list = []
    for lock in rule_locks:
        if lock['state'] != 'OK' :
            #print(lock)
            stuck_locks_list.append(lock)
    print(f"Testing rule {ruleid} with {len(stuck_locks_list)} stuck locks")
    for lock in stuck_locks_list:
        stuck_file = StuckFile(ruleid=ruleid, lfn=lock['name'],\
                               rule_rse_expression=rule_rse_expression)
        test_if_it_is_last_file_replica(lock, stuck_file)
    return None

def test_if_it_is_last_file_replica(lock, stuck_file):
    """Check and count AVAILABLE and UNAVAILABLE replicas"""
    filename = lock['name']
    stuck_file.filename = filename
    file_did = {"scope":SCOPE, "name":filename}
    stuck_file.did = file_did

    file_replicas_all_states = list(client.list_replicas( 
        [file_did], all_states=True ))
    replicas_rses = list(file_replicas_all_states[0]["states"].keys())
    rucio_adler32 = file_replicas_all_states[0]['adler32']
    stuck_file.rucio_adler32 = rucio_adler32

    counter_availables = 0
    counter_unavailables = 0
    pfn_sources_to_test = []
    last_replica_rse = None

    for rse in replicas_rses:
        if file_replicas_all_states[0]["states"][rse] == "AVAILABLE":
            counter_availables += 1
            last_replica_rse = rse
            pfn_sources_to_test.append( 
                file_replicas_all_states[0]["rses"][rse] )
            
        if file_replicas_all_states[0]["states"][rse] == "UNAVAILABLE":
            counter_unavailables += 1

    if counter_availables == 1: # Yes, it is an AVAILABLE last replica!
        print("Single replica AVAILABLE found, testing if its OK")
        pfn = pfn_sources_to_test[0][0]
        stuck_file.last_replica_rse = last_replica_rse
        stuck_file.pfn = pfn
        pprint(stuck_file.__dict__)

        if (last_replica_rse != 'T2_IT_BARI') and \
            (stuck_file.rule_rse_expression == 'T1_US_FNAL_Tape'):
            print("BARI & FNAL_Tape!!!\n Create kick rule to T1_UK_RAL_Disk\n")
            dst_rse = 'T1_UK_RAL_Disk'
            lifetime = str(86400*2)
            comments = f'Kick rule to T1_US_FNAL_Tape by {OPERATOR}'
            copies = 1
            s = subprocess.run(['rucio', 'add-rule', '--lifetime', lifetime,'--comment', comments, str("cms:"+filename), str(copies), dst_rse, '--asynchronous'], stdout= subprocess.PIPE, stderr = subprocess.PIPE, check=False)
            print(s.stdout.decode('UTF-8'))

            print("Create kick rule to T1_FNAL_Disk\n")
            src_rse='T1_UK_RAL_Disk'
            dst_rse='T1_US_FNAL_Tape'
            comments = f'Kick rule to T1_US_FNAL_Tape by {OPERATOR}'
            copies = 1
            s = subprocess.run(['rucio', 'add-rule', '--lifetime', lifetime,'--comment', comments, str("cms:"+filename), str(copies), dst_rse, '--asynchronous', '--delay--injection', str(600), '--source-replica-expression', src_rse], stdout= subprocess.PIPE, stderr = subprocess.PIPE, check=False)
            print(s.stdout.decode('UTF-8'))

        else:
            print("Let's test if it is a corrupted replicas (not BARI/FNAL)")
        # DO GFAL_COPY, gfal-sum ADLER32 and Rucio DeclareBadReplica
        #is_corrupted_replica(pfn, rucio_adler32, filename, last_replica_rse)

    elif counter_unavailables == len(replicas_rses): # No, all replicas are 
                                                     # UNAVAILABLE
        print("All replicas are UNAVAILABLE, declaring all of them as bad.")
        for rse in replicas_rses:
            declare = client.declare_bad_did_replicas(rse=rse, dids=[file_did],
                        reason=REASON_ALL_INVALID)
            print(f"{filename},{rse},SUCCESS file INVALIDATED,{declare}")
            pprint(stuck_file.__dict__)

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

        if r.returncode == 2 : 
            print(f"file not found in last replica rse. {filename}")
            print("proceeding with last replica invalidation")
            declare = client.declare_bad_did_replicas(rse=last_replica_rse,
                                              dids=[file_did], 
                                              reason=REASON_LAST_CORRUPTED)
            print(f"{filename},{last_replica_rse},SUCCESS file INVALIDATED,{declare}")

            return None
    result = r.stdout.decode('UTF-8').splitlines()[0].split()
    local_adler32 = result[1]

    if local_adler32 != rucio_adler32:
    # Candidate elegible for Last Replica Corrupted
        declare = client.declare_bad_did_replicas(rse=last_replica_rse,
                                              dids=[file_did], reason=REASON_LAST_CORRUPTED)
        print(f"{filename},{last_replica_rse},SUCCESS file INVALIDATED,{declare}")

    return None

def last_replica_in_T2_IT_BARI():
    pass

def read_rules_from_file(list_of_rules):
    """
    Open the list of files specified on fi
    """
    with open(list_of_rules, 'r', encoding='utf-8') as f:
        rule_list = f.read().splitlines()
        pprint(rule_list)
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
        #subprocess.call(['rucio', 'update-rule', ruleid , '--stuck',\
         #                "--boost-rule"])

    return 0

if __name__ == "__main__":
    main()
