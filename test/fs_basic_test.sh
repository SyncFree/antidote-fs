#!/bin/bash
# Simple script to test basic file system operations.

set -e

[ $# -eq 0 ] && mkdir -p d1 && ROOT=./d1 || ROOT=$1

. ./test/utils.sh

echo "Start file system basic test"

pushd $ROOT

FILE="file_$(rnd_str)"
DIR="dir_$(rnd_str)"
CONT="Hello world!\n"

touch $FILE
echo -n "File creation.................."
if [ -f $FILE ]; then ok; else ko; fi

rm $FILE
echo -n "File deletion.................."
if [ ! -f $FILE ]; then ok; else ko; fi

mkdir $DIR
echo -n "Dir creation..................."
if [ -d $DIR ]; then ok; else ko; fi

rmdir $DIR
echo -n "Empty dir deletion............."
if [ ! -d $DIR ]; then ok; else ko; fi

echo -e $CONT > $FILE
echo -n "File write....................."
if [[ -f $FILE && $(< $FILE) == $(echo -e "$CONT") ]]; 
then ok; else ko; fi

sed -i 's/world/mondo/' $FILE
echo -n "File update...................."
if [[ -f $FILE && $(< $FILE) == $(echo -e "Hello mondo!\n") ]]; 
then ok; else ko; fi

mkdir $DIR
FILE1=$FILE"_1.txt"; echo hello > $DIR/$FILE1
FILE2=$FILE"_2.txt"; echo world > $DIR/$FILE2
echo -n "Creation of files inside dir..."
if [[ $(cat "$DIR/$FILE"_{1,2}.txt) == $(echo -e "hello\nworld") ]]; 
then ok; else ko; fi

rm -rf $DIR
echo -n "Non-empty dir deletion........."
if [ ! -d $DIR ]; then ok; else ko; fi

touch $FILE; mv $FILE "$FILE"_new
echo -n "Rename file...................."
if [[ -f "$FILE"_new && ! -f $FILE ]]; then ok; else ko; fi

echo -e $CONT > $FILE; mkdir $DIR; mv $FILE $DIR/"$FILE"_new
echo -n "Move file......................"
if [ -f $DIR/"$FILE"_new ] && [[ $(cat "$DIR/$FILE"_new) == $(echo -e $CONT) ]]
then ok; else ko; fi

mkdir -p $DIR; mv $DIR "$DIR"_new
echo -n "Rename empty dir..............."
if [[ ! -d $DIR && -d "$DIR"_new ]]
then ok; else ko; fi

mkdir -p $DIR; touch $DIR/$FILE; mv $DIR "$DIR"_new
echo -n "Rename non-empty dir..........."
if [[ ! -d $DIR && -d "$DIR"_new ]]
then ok; else ko; fi

mkdir -p $DIR "$DIR"2; mv $DIR "$DIR"2
echo -n "Move empty dir................."
if [[ ! -d $DIR && -d "$DIR"2/$DIR ]]
then ok; else ko; fi

mkdir -p $DIR "$DIR"2; touch $DIR/$FILE; mv $DIR "$DIR"2
echo -n "Move non-empty dir............."
if [[ ! -d $DIR && -f "$DIR"2/$DIR/$FILE ]]
then ok; else ko; fi

echo "hello" > $FILE; ln -s $FILE "$FILE"_slink; 
echo -n "File soft linking.............."
if [[ -L "$FILE"_slink && $(< "$FILE"_slink) == $(echo "hello") ]]
then ok; else ko; fi

echo "hello" > $FILE; ln $FILE "$FILE"_hlink;
echo -n "File hard linking.............."
if [[ $(stat -c %h "$FILE"_hlink) -eq 2 && 
    $(< "$FILE"_hlink) == $(echo "hello") && 
    $(stat -c %i "$FILE"_hlink) -eq $(stat -c %i "$FILE") ]]
then ok; else ko; fi

touch "$FILE"_1; chmod 750 "$FILE"_1; 
touch "$FILE"_2; chmod 703 "$FILE"_2;
touch "$FILE"_3; chmod 755 "$FILE"_3;
echo -n "File permissions..............."
if [[ $(stat -c %a "$FILE"_1) -eq 750 && 
    $(stat -c %a "$FILE"_2) -eq 703 &&
    $(stat -c %a "$FILE"_3) -eq 755 ]]
then ok; else ko; fi

rm -rf $FILE* $DIR*

popd;

