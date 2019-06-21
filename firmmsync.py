#!/usr/bin/env python3
# firmmsync.py
# Created: Apr. 2, 2019
# Purpose: Transfer fMRI (EPI) images from the GE console to the local host as
#   soon as they are acquired. For use with the FIRMM motion detection software.
# Action:
#  Start the program after finishing the series just prior to the fMRI.
#  It will then create a directory which FIRMM can select for the incoming DICOM.
#  Press a key to initiate the polling that this script will do to look for new
#  DICOM images (assumed to be fMRI frames).
#  The new images will be transferred to the local host as soon as a new DICOM directory
#  is detected in the console's database.
#  First an rsync is made to a temporary directory before a copy to FIRMM's incoming
#  directory to minimize the chance that FIRMM will detect & use a partial frame.

# Usage: Run ./firmmsync.py from the host where FIRMM will be running.
#   (all the pathnames and server details are hard-coded).
#
# Updates:

# Author: Perry E. Radau
# Email: perry.radau1@ucalgary.ca
# License: MIT
# Copyright 2019 Perry E. Radau
# https://opensource.org/licenses/MIT

import os
from datetime import datetime
import string
import shutil
import subprocess
import sys
import shlex
from time import sleep
import pydicom as dicom
import signal

OK=0
incomingdir = ""


def get_dcmpath(srcdir):
    if os.path.isdir(srcdir):
        #Assumes a GE dicom with "i" beginning each filename.
        list_dcm = [ x for x in os.listdir( srcdir ) if (os.path.isfile(os.path.join(srcdir,x)) and x.startswith("i")) ]
        if list_dcm:
            dcmfile = list_dcm[0]
            print(dcmfile)
            return os.path.join(srcdir, dcmfile)
        else:
            print("No DICOM file found in: %s\n Must exit." % srcdir )
            sys.exit(0)
    else:
        print("No such source directory for DICOM: %s\n Must exit." % srcdir)
        sys.exit(0)


#read a dicom file and return its StudyID and selected metadata as a dictionary
#  This conversion makes the data easier to use.
def get_metadata(dcmpath):
    print('Trying to read as DICOM the file here:', dcmpath)
    # tag_list = [ "StudyID", "PatientID", "PatientName", "ProtocolName", "SeriesDescription", "SeriesDate", "SeriesTime"]
    tag_list = [ "StudyID", "PatientID", "PatientName", "ProtocolName", "SeriesDescription", "SeriesDate", "SeriesTime"]
    #read the desired tags from the DICOM file, skipping other metadata and all pixel data.
    with dicom.dcmread(dcmpath, stop_before_pixels=True, specific_tags=tag_list) as ds:
        #if some tags are missing, replace with null to avoid later exceptions.
        if len(ds) != len(tag_list):
            for tag in tag_list:
                if not (tag in ds):
                    # print('must add tag', tag)
                    if tag == "StudyID":
                        ds.StudyID = "null"
                    elif tag == "PatientID":
                        ds.PatientID = "null"
                    elif tag == "PatientName":
                        ds.PatientName = "null"
                    elif tag == "ProtocolName":
                        ds.ProtocolName = "null"
                    elif tag == "SeriesDescription":
                        ds.SerieDescription = "null"
                    elif tag == "SeriesDate":
                        ds.SeriesDate = "null"
                    elif tag == "SeriesTime":
                        ds.SeriesTime = "null"

        metadata_list = [
            ds.StudyID,
            ds.PatientID,
            ds.PatientName,
            ds.ProtocolName,
            ds.SeriesDescription,
            ds.SeriesDate,
            ds.SeriesTime]

        #put the tags into a dictionary, with values converted to strings
        metadata = {tag_list[j]: str(x) for (j,x) in enumerate(metadata_list)}

    return metadata


#display the study metadata from the DICOM
def print_studydata(metadata):
    print("Study ID: %s" % metadata["StudyID"] )
    print("Patient ID: %s" % metadata["PatientID"] )
    # Not showing patient name b/c it often has confidential info.
    # print("Patient Name: %s" % metadata["PatientName"] )
    print("Series Description: %s" % metadata["SeriesDescription"] )
    print("Series Date: %s" % metadata["SeriesDate"] )
    print("Series Time: %s" % metadata["SeriesTime"] )
    print("Protocol: %s" % metadata["ProtocolName"] )



# Execute a command line call constructed from cmdestdirr, returning 0 on flagess (else error code).
def systemcall ( cmdestdirr ):
    ''' System call to execute command string in a shell. '''
    try:
        retcode = subprocess.call( cmdestdirr, shell=True)
        if retcode != OK:
            print ("Error code:", retcode) #, file = sys.stderr) )
        return retcode

    except OSError as e:
        print ("Execution failed:", e) #, file = sys.stderr) )



def systemcall_pipe( cmdstr, allow=None ):
    ''' System call to execute command string, to get stderr and stdout output in variable proc. '''
    # this function is superior to systemcall for use with Spyder where otherwise stdout/stderr are not visible.
    # it is also needed if your main program needs to capture this output instead of only print it to terminal.
    args = shlex.split(cmdstr)
    try:
        proc = subprocess.Popen(args, stdout=subprocess.PIPE, stderr=subprocess.PIPE)
        #stdout and stderr from your process
        out, err = proc.communicate()
        retcode = proc.returncode
        if err:
            #decode the standard errors to readable form
            str_err = err.decode("utf-8")
            #Exclude error messages in allow list which are expected.
            bShow = True
            if allow:
                for allowstr in allow:
                    if allowstr in str_err:
                        bShow = False
            if bShow:
                print ("System command '{0}' produced stderr message:\n{1}".format(cmdstr, str_err))

        str_out = out.decode("utf-8")
        if str_out:
            print ("System command '{0}' produced stdout message:\n{1}".format(cmdstr, str_out))

        return retcode, str_out
    except OSError as e:
        print ("Execution failed:", e )


#connect to the remote server (userAThost) and find the most recent image.
# This assumes that rootdir is pointing at the GE style folder hierarchy of patient,exam,series and images.
def get_last_imagepath(userAThost, rootdir):
    flag = OK
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + rootdir + " -rt | tail -n 1" )
    #need to strip newline characters
    ptdir = os.path.join(rootdir, out.strip())
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + ptdir + " -rt | tail -n 1" )
    if out[0] != "e":
        print("Exam not found")
        flag = -1

    examdir = os.path.join(ptdir, out.strip())
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + examdir + " -rt | tail -n 1" )
    if out[0] != "s":
        print("Series not found")
        flag = -1

    seriesdir = os.path.join(examdir, out.strip())
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + seriesdir + " -rt | tail -n 1" )
    if out[0] != "i":
        print("Images not found")
        flag = -1
    else:
        #strip whitespace and trailing '*' which is picked up from 'ls' command output.
        out = out.strip().strip('*')

    imagepath = os.path.join(seriesdir, out)
    return flag, imagepath, examdir



def get_last_seriespath(userAThost, examdir):
    flag = OK
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + examdir + " -rt | tail -n 1" )
    seriesdir = os.path.join(examdir, out.strip())
    if out[0] != "s":
        print("Series not found")
        flag = -1
    return flag, seriesdir


#catch keyboard interrupts typically while looping endlessly, and print out
#  study information for the series that was transferred, then QUIT.
def signal_handler(sig, frame):
    print('Process intentionally STOPPED. (Ctrl-C)')
    global incomingdir
    filepath = get_dcmpath(incomingdir)
    metadata = get_metadata(filepath)
    print_studydata(metadata)
    print('Quitting!')
    sys.exit(0)


def check_for_fmri(userAThost, remotedir, localdir ):
    #find most recent sample DICOM in remotedir
    retcode, out = systemcall_pipe( "ssh " + userAThost + " ls " + remotedir + " -rt | tail -n 1" )
    if out[0] != "i":
        print("Images not found")
        flag = -1
    else:
        #strip whitespace and trailing '*' which is picked up from 'ls' command output.
        out = out.strip().strip('*')

    imagepath = os.path.join(remotedir, out)

    #transfer the sample file
    flag = "ptgvhP"
    cmd = "/usr/bin/rsync -" + flag + " --no-links  --ignore-existing " + userAThost + ":" + imagepath + " " + localdir + "/"
    print("cmd: %s" % cmd)
    if systemcall( cmd ) == OK:
        print( "rsync of this file succeeded:\n  %s" % imagepath )
        print( "destination dir:\n %s" % localdir )
    else:
        print( "rsync of this file failed:\n  %s" % imagepath )
        sys.exit(0)
    #check that the sample file has appropriate metadata
    filepath = get_dcmpath(localdir)
    print('filepath', filepath)
    testmetadata = get_metadata(filepath)
    print('Found series: %s' % testmetadata['SeriesDescription'])
    #Check that the new sequence is fMRI, assuming 'fMRI' or 'EPI'
    if testmetadata['SeriesDescription'].startswith('fMRI') or testmetadata['SeriesDescription'].startswith('EPI'):
        print("fMRI found!")
        return True
    else:
        print("No fMRI found.")
        return False



def main():
    ver = sys.version_info[0]
    print("Using Python version", ver)
    if ver < 3:
        raise Exception("Must be using Python 3")

    #bVerbose flag to indicate if the user gets extra info printed to the terminal.
    bVerbose = True

    #username and IP of GE console computer
    user = "sdc"
    host = "172.22.13.7"
    userAThost = user + "@" + host
    #top level directory containing the DICOMs on the console.
    rootdir = "/export/home1/sdc_image_pool/images"

    flag, origimagepath, examdir = get_last_imagepath(userAThost, rootdir)
    print("origimagepath: %s" % origimagepath)

    print("examdir: %s" % examdir)

    flag, orig_seriesdir = get_last_seriespath(userAThost, examdir)
    print("orig_seriesdir: %s" % orig_seriesdir)

    #current date without time.
    now_list = str(datetime.now()).split(" ")
    curr_time = now_list[1][:8].replace(":","_")
    curr_date = now_list[0][:10]
    outdir = "firmmsync_" + curr_date + "_" + curr_time
    #home directory
    home = os.path.expanduser("~")
    tempdir = os.path.join(home, "temp")
    # destdir = os.path.join(tempdir, outdir)
    global incomingdir
    incomingdir = os.path.join(home, "FIRMM", "incoming_DICOM", outdir)
    if not os.path.isdir(tempdir):
      os.mkdir(tempdir)
    if not os.path.isdir(incomingdir):
      os.mkdir(incomingdir)

    #WAIT for input here before proceeding to search for new directories.
    input("Press return to begin polling for new DICOMs...")

    bFound = False
    srcdir = seriesdir = orig_seriesdir
    s = 0
    while not bFound:
        s = s + 1
        if bVerbose:
            print("waiting...current seriesdir: %s" % seriesdir)
        flag, seriesdir = get_last_seriespath(userAThost, examdir)

        #if the latest image directory has changed then check to exit loop
        if seriesdir != orig_seriesdir:
            bFound = check_for_fmri(userAThost, seriesdir, tempdir )
            if bFound:
                if bVerbose:
                    print("fMRI found")
                srcdir = seriesdir

    if bVerbose:
      print ("Found a new series!")
      print ("return code: %s " % str(flag))
      print ("dir to copy: %s" % seriesdir)

    signal.signal(signal.SIGINT, signal_handler)

    print("### Copying files ...")
    user = "firmmproc"

    flag = "ptg"
    if bVerbose:
        flag = flag + "vhP"
    #-T (temp-dir) flag is used to put the partial transferred files somewhere other than the incoming directory for FIRMM.
    cmd = "/usr/bin/rsync -" + flag + " --no-links  --ignore-existing -T " + tempdir + " " + userAThost + ":" + srcdir + "* " + incomingdir + "/"
    print("rsync cmd: %s" % cmd)

    #Sleep interval between rsync is not exact, and .001 (1ms) is likely the
    #  minimum the system can provide. Intended to prevent overlapping rsyncs.
    #  The purpose is to ensure this loop is not overloading the console / network (like a DOS attack)
    time_inc = 0.001
    i=0
    while True:
        print("iter: %d" % i)
        if systemcall( cmd ) == OK:
            if bVerbose:
                print( "rsync of this dir succeeded:\n  %s" % srcdir )
                print( "destination dir:\n %s" % incomingdir )

        else:
            print( "rsync of this dir failed:\n  %s" % incomingdir )
            sys.exit(0)

        i += 1
        sleep(time_inc)

    if bVerbose:
        #print some info about the series transferred
        if not metadata:
            filepath = get_dcmpath(incomingdir)
            metadata = get_metadata(filepath)

        print("Series transferred:")
        print_studydata(metadata)

    print("DONE!")


if __name__ == "__main__":
    main()
