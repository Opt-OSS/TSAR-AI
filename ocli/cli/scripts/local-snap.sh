#!/usr/bin/env bash
auxdata=$HOME/.snap
gpt_cache='40G'
dry_run=false
local_path=$(dirname "$0")
#echo $@
# As long as there is at least one more argument, keep looping
while [[ $# -gt 1 ]]; do
    key="$1"
    case "$key" in
        #-----------------------------------
        --dry-run*)         dry_run=true                           ;;
        --gpt-cache)         shift; gpt_cache=$1                   ;;
        --gpt-cache=*)       gpt_cache="${key#*=}"                 ;;
        --eodata)         shift; eodata=$1                         ;;
        --eodata=*)       eodata="${key#*=}"                       ;;
        --snap_results)   shift; snap_results=$1                   ;;
        --snap_results=*) snap_results="${key#*=}"                 ;;
        --master)   shift; master=$1                               ;;
        --master=*) master="${key#*=}"                             ;;
        --slave)   shift; slave=$1                                 ;;
        --slave=*) slave="${key#*=}"                               ;;
        --swath)   shift; swath=$1                                 ;;
        --swath=*) swath="${key#*=}"                               ;;
        --firstBurstIndex)   shift; firstBurstIndex=$1             ;;
        --firstBurstIndex=*) firstBurstIndex="${key#*=}"           ;;
        --lastBurstIndex)   shift; lastBurstIndex=$1               ;;
        --lastBurstIndex=*) lastBurstIndex="${key#*=}"             ;;
        #-----------------------------------
        *)
            if [[ ${key} == -* ]];
            then
                echo
                echo "Unknown option $key"
                echo
                usage
            fi
            ;;
    esac
    shift
    # Shift after checking all the cases to get the next option
done
#
#mkdir -p  $auxdata
#chmod 777  $auxdata
CMD="gpt -e -c $gpt_cache  $local_path/Sig-Coh-Stack-VH-VV-FIN-orb.xml -Smaster=$master -Sslave=$slave \
  -Pswath=$swath -PfirstBurstIndex=$firstBurstIndex -PlastBurstIndex=$lastBurstIndex  \
  -Pout=$snap_results
"

# Zsh and other crazy shells extends patterns passed arguments (glob), so be sure rsync runs in BASH!!!
if  [[ "$dry_run" == true ]];then
    echo "Snap cache in $auxdata"
    echo "$CMD"
else
#    echo "exec $dry_run"
    bash -c "$CMD"
fi


