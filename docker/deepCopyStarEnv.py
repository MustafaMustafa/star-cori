#!/usr/bin/python
__author__ = "Mustafa Mustafa"
__email__ = "mmustafa@lbl.gov"
""" a script to create a deep copy of STAR environment """

import os, sys
import argparse
import subprocess

def get_args():
    parser = argparse.ArgumentParser(description='Script to create a deep copy of STAR environment')
    parser.add_argument('--opt', help='create OPT tarball', action = 'store_true', default = False)
    parser.add_argument('--root', help='create root tarball', action = 'store_true', default = False)
    parser.add_argument('--SL', help='create star library tarball', action = 'store_true', default = False)
    parser.add_argument('--all', help='create tarballs of OPT, root and SL. Default behavior.', action = 'store_true', default = True)
    args = parser.parse_args()

    if args.opt or args.root or args.SL:
        args.all = False
    else:
        args.opt = True
        args.root = True
        args.SL = True

    return args

def get_star_env_vars(args):

    # check if in STAR environment
    if subprocess.call(['which','starver']):
        print >> sys.stderr, "ERROR: cannot find starver command, make sure you are running in a STAR enviroment"
        exit(1)

    star_vars = {}
    star_vars['STAR_VERSION'] = os.environ['STAR_VERSION']
    star_vars['STAR_HOST_SYS'] = os.environ['STAR_HOST_SYS']
    star_vars['ROOT_LEVEL'] = os.environ['ROOT_LEVEL']

    star_vars['opt'] = '/opt/star'
    star_vars['sl'] = os.path.realpath('%s/%s'%(os.environ['STAR_PATH'], os.environ['STAR_VERSION']))
    star_vars['STAR_ROOT'] = os.environ['STAR_ROOT']
    star_vars['root'] = 'ROOT/%s/.%s'%(os.environ['ROOT_LEVEL'],os.environ['STAR_HOST_SYS'])

    return star_vars

def create_opt_tarball(star_vars):
    print "Creating a tarball of STAR OPT..."
    tarball_name = 'optstar.%s.tar.gz'%star_vars['STAR_HOST_SYS']

    if os.path.isfile(tarball_name):
        print "File %s already exists. Skipping ..."%tarball_name
        return 1

    fail = subprocess.call( [ 'tar','-C',star_vars['opt'],'-czhf',tarball_name,star_vars['STAR_HOST_SYS'] ] )

    if fail:
        print "WARNING : Creating OPT tarball failed!"
    else:
        print "Successfully created a tarball of STAR OPT"

    return fail

def create_root_tarball(star_vars):
    print "Creating a tarball of ROOT..."
    tarball_name = 'rootdeb-%s.%s.tar.gz'%(star_vars['ROOT_LEVEL'],star_vars['STAR_HOST_SYS'])

    if os.path.isfile(tarball_name):
        print "File %s already exists. Skipping ..."%tarball_name
        return 1

    fail = subprocess.call( [ 'tar','-C',star_vars['STAR_ROOT'],'-czhf',tarball_name,star_vars['root'] ] )

    if fail:
        print "WARNING : Creating ROOT tarball failed!"
    else:
        print "Successfully created a tarball of ROOT"

    return fail

def create_star_tarball(star_vars):
    print "Creating a tarball of STAR Library"
    tarball_name = '%s.tar'%(star_vars['STAR_VERSION'])
    gz_name = '%s.tar.gz'%(star_vars['STAR_VERSION'])

    if os.path.isfile(gz_name):
        print "File %s already exists. Skipping ..."%gz_name
        return 1

    subprocess.call( ['mkdir', star_vars['STAR_VERSION'] ] )
    subprocess.call( 'cp -rp %s/* %s'%(star_vars['sl'], star_vars['STAR_VERSION']), shell=True)
    subprocess.call( ['cp', '-rp', '%s/.%s'%(star_vars['sl'],star_vars['STAR_HOST_SYS']), star_vars['STAR_VERSION']] )
    subprocess.call( ['tar', '-czf', gz_name, star_vars['STAR_VERSION'] ] )
    # fail = subprocess.Popen( [ 'tar','-cf',tarball_name,"%s/*"%star_vars['sl'] ] , shell=True).wait()
    # fail += subprocess.call( [ 'gunzip','tarball_name'] )

    if fail:
        print "WARNING : Creating STAR Library tarball failed!"
    else:
        # subprocess.call( ['rm', '-rf', star_vars['STAR_VERSION']] )
        print "Successfully created a tarball of ROOT"

    return fail

def main():
    args = get_args()
    star_vars = get_star_env_vars(args)

    print "STAR_VERSION = %s"%star_vars['STAR_VERSION']
    print "ROOT_LEVEL = %s"%star_vars['ROOT_LEVEL']
    print "OPT = %s/%s"%(star_vars['opt'], star_vars['STAR_HOST_SYS'])

    if args.opt:
        create_opt_tarball(star_vars)

    if args.root:
        create_root_tarball(star_vars)

    if args.SL:
        create_star_tarball(star_vars)

if __name__ == '__main__':
    main()
