for i in $@
do
	arg=" $arg $i"
done

g++ -std=c++0x ext_merge.cpp
./a.out $arg
