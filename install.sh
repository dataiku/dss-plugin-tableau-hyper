#! /bin/sh -e

echo "Starting Tableau plugin installation procedure"

if [ -z "$DIP_HOME" ]; then
	echo "Missing DIP_HOME"
	exit 1
fi
if [ -z "$DKUINSTALLDIR" ]; then
	echo "Missing DKUINSTALLDIR"
	exit 1
fi


DISTRIBFINDSH=$DKUINSTALLDIR/common/scripts/_find-distrib.sh

if [ ! -f "$DISTRIBFINDSH" ]; then
    echo "Missing distribution finder script: $DISTRIBFINDSH, DSS may be too old, trying another location"
    DISTRIBFINDSH=$DKUINSTALLDIR/scripts/_find-distrib.sh
    if [ ! -f "$DISTRIBFINDSH" ]; then
        echo "Missing distribution finder script: $DISTRIBFINDSH, maybe in a dev env ?"
        DISTRIBFINDSH=$DKUINSTALLDIR/packagers/common/scripts/_find-distrib.sh
        if [ ! -f "$DISTRIBFINDSH" ]; then
            echo "Missing distribution finder script: $DISTRIBFINDSH, exiting"
            exit 1
        fi
    fi
fi

echo "Found distrib finder at: $DISTRIBFINDSH"

detectedDistrib=$($DISTRIBFINDSH)
distrib=`echo "$detectedDistrib"|cut -d ' ' -f 1`

USEDPYTHONBIN=$DIP_HOME/code-envs/python/plugin_tableau-hyper-export_managed/bin

echo "Used python bin is $USEDPYTHONBIN"

if [ ! -d "$USEDPYTHONBIN" ]; then
	echo "Plugin codenv plugin_tableau-hyper-export_managed does not exist"
    echo "You may want to create it or make sure the name is correct"
	exit 1
fi

isHyper=$(
$USEDPYTHONBIN/python - <<EOF
try:
    import tableausdk.HyperExtract
    print 1
except Exception:
    print 0
EOF
)
echo "is hyper is $isHyper"

#we now want to always install
#if [ "$isHyper" == 1 ]; then
#    echo "Tableau SDK HyperExtract package already installed, skipping installation"
#    exit 0
#else
#    echo "Installing Tableau extract api"
#fi

TMPDIR=`mktemp -d $DIP_HOME/tmp/tableau-install-XXXXXX`
cd $TMPDIR
echo $TMPDIR

case "$distrib" in
  osx)
    echo "Mac OS distribution, building before installing"
    curl "https://downloads.tableau.com/tssoftware/extractapi-py-osx64-2018-2-0.tar.gz" |tar --strip-components 1 -x -z -f -
    $USEDPYTHONBIN/python setup.py build
    ;;
  debian | ubuntu)
    echo "Debian/Ubuntu distribution, building before installing"
    curl "https://downloads.tableau.com/tssoftware/extractapi-py-linux-x86_64-2018-2-0.tar.gz" |tar --strip-components 1 -x -z -f -
    $USEDPYTHONBIN/python setup.py build
    ;;
  centos | redhat)
    echo "Centos/RHEL distribution, skipping build that is buggy with Tableau SDK package"
    curl "https://downloads.tableau.com/tssoftware/extractapi-py-linux-x86_64-2018-2-0.tar.gz" |tar --strip-components 1 -x -z -f -
    ;;
  *)
    echo "Distribution $distrib not supported exiting"
    exit 1
    ;;
esac

$USEDPYTHONBIN/python setup.py install

