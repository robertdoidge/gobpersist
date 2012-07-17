#!/bin/bash

# Script to build rpm for gobpersist
# This script has to run from the source dir
#
# Usage: build-rpm.sh clean|tgz|rpm

######################
# VARIABLES DEFINITION
######################

NAME="gobpersist"
SPEC_FILE="$NAME.spec"
FILELIST=".filelist"

PWD=$(pwd)
SOURCE_DIR=$PWD
LOG_DIR="$PWD/log"
LOG_FILE="$LOG_DIR/$NAME.log"
DIST_DIR="$PWD/dist"
BUILD_DIR="$PWD/build"
BUILD_SUBDIRS="BUILD RPMS SPECS SOURCES SRPMS TMP install"

MAJOR_VERSION=$(grep " version " $SPEC_FILE | sed "s/.* //")
MINOR_REV=$(grep " release " $SPEC_FILE | sed "s/.* //")
TGZ_WORK_DIR="$NAME-$MAJOR_VERSION"
TGZ_FILE="$NAME-$MAJOR_VERSION.tar.gz"
RPM_FILE="$NAME-$MAJOR_VERSION-$MINOR_REV.noarch.rpm"


######################
# FUNCTIONS DEFINITION
######################

clean(){
	echo "Cleaning $PWD..."
	rm -rfv $BUILD_DIR $DIST_DIR $TGZ_WORK_DIR $FILELIST
}


build_tgz(){
        if test -f $SPEC_FILE;then
                echo "Working on: $TGZ_FILE"
        else
                echo "Spec file not found: $SPEC_FILE"
                exit
        fi
	echo "Creating $DIST_DIR"
	mkdir -p "$DIST_DIR"

        # script has to run from $SOURCE_DIR
	cd $SOURCE_DIR && find . -type f | grep "/gobpersist/" | grep -v "test.py" | grep -v "TODO" | grep -v "sample.txt" | xargs -i echo $TGZ_WORK_DIR/{} > $FILELIST
        ln -sf . $TGZ_WORK_DIR
        tar -czf - -T $FILELIST > $TGZ_FILE
        mv $TGZ_FILE $DIST_DIR
	echo "Archive is created: $DIST_DIR/$TGZ_FILE"
}


make_rpm(){
        echo "Working on rpm: $RPM_FILE"
	if test -d "$LOG_DIR"; then
		echo "Log directory $LOG_DIR already exists."
	else
		echo "Creating log directory $LOG_DIR"
		mkdir "$LOG_DIR"
	fi

	echo "Creating build directory $BUILD_DIR and its subdirs... "
	for i in `echo $BUILD_SUBDIRS`
	do
		mkdir -p "$BUILD_DIR/$i"
	done

        ln -sf $DIST_DIR/$TGZ_FILE $BUILD_DIR/SOURCES/
        cp $SPEC_FILE $BUILD_DIR/SPECS/
        rpmbuild -bb \
            --define "_topdir $BUILD_DIR" \
            --define "buildroot $BUILD_DIR/install" \
            $RPMOPT \
            $BUILD_DIR/SPECS/$SPEC_FILE \
	> $LOG_FILE 2>&1
        full_path_rpm="$BUILD_DIR/RPMS/noarch/$RPM_FILE" 
        if test -f "$full_path_rpm";then 
		mv $full_path_rpm $DIST_DIR
		echo "RPM is created: $DIST_DIR/$RPM_FILE"
	else
        	echo "rpmbuild failed, $full_path_rpm not found. Please check and try again."
		exit
	fi
}


sign_rpm() {
        echo "Signing process is not implemented yet..."
        #if test -f $DIST_DIR/$RPM_FILE;then
        #        echo "You have: " ; ls -l $DIST_DIR/*.rpm
        #else
        #        echo "RPM file not exist: $RPM_FILE"
        #        exit
        #fi

	#HOST=`hostname`
	#BUILDHOST="bob.sd.dev"
	#if test "$HOST" = $BUILDHOST; then
        #	ls dist/RPMS/i386/$RPM_FILE | awk '{system(" /root/bin/signrpm.exp " $$1)}'
	#else
	#	echo "we can sign this only at build server"
	#	exit
	#fi

        #rm -vf /mnt/home/budiap/download/Accellion/FTA/5.0_testing/RPMS/$NAME-*rpm
        #cp -v dist/RPMS/i386/$RPM_FILE /mnt/home/budiap/download/Accellion/FTA/5.0_testing/RPMS/
        #chown budiap.500 /mnt/home/budiap/download/Accellion/FTA/5.0_testing/RPMS/$RPM_FILE

        #echo "Building yum signatures..."
        #su - budiap -c "cd /mnt/home/budiap/download/Accellion/FTA/5.0_testing; echo -n Running_on_;hostname; sh yum-arch.build"
}



######################
# MAIN
######################

case "$1" in
        clean)
		clean
        ;;
	rpm)
		clean
		build_tgz
		make_rpm
	;;
        tgz)
                clean
                build_tgz
        ;;
	sign)
		sign_rpm
	;;
	*)
		echo "Usage: $0 <options>"
		echo "options:"
		echo "clean => clean the build and dist directories"
		echo "rpm => create the rpm package" 
		echo "tgz => create the tgz archive" 
		echo "sign => currently not implemented"

	exit 1
esac
