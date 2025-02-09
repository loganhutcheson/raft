test=$1
if [ -z $1 ]; then
	echo "please provide test name"
	exit 1
fi

while true; do
	VERBOSE=1 go test -run=$1 > output.log
	if grep -i fail output.log; then
		echo "failed"
		exit 0
	else
		echo "pass"
	fi
done
