/*
Name:  Sourabh Saha, Syed Zawad, Joseph Giallo
Unity id:  sssaha2, sazawad, jfgiall2
*/

/*
Citations:
FUSE Tutorial 1 from:
http://www.cs.nmsu.edu/~pfeiffer/fuse-tutorial/

FUSE Tutorial 2 from:
http://www.maastaar.net/fuse/linux/filesystem/c/2016/05/21/writing-a-simple-filesystem-using-fuse/

More FUSE Resources:
https://lastlog.de/misc/fuse-doc/doc/html/
https://github.com/libfuse/libfuse

LinkedList Adapted from:
https://www.tutorialspoint.com/data_structures_algorithms/linked_list_program_in_c.htm

Relevant Manual Pages:
https://linux.die.net/man/
*/

/*
  Key Value System
  Copyright (C) 2016 Hung-Wei Tseng, Ph.D. <hungwei_tseng@ncsu.edu>
  This program can be distributed under the terms of the GNU GPLv3.
  See the file COPYING.
  This code is derived from function prototypes found /usr/include/fuse/fuse.h
  Copyright (C) 2001-2007  Miklos Szeredi <miklos@szeredi.hu>
  His code is licensed under the LGPLv2.
  A copy of that code is included in the file fuse.h
  
  The point of this FUSE filesystem is to provide an introduction to
  FUSE.  It was my first FUSE filesystem as I got to know the
  software; hopefully, the comments in this code will help people who
  follow later to get a gentler introduction.
*/
#include "kvfs.h"
#include "log.h"
#include <unistd.h>
#include <sys/types.h>
#include <dirent.h>
#include <stdio.h>
#include <ftw.h>

int first_run = 0;

typedef struct listNode{
    char* hashedVal;
    char* fileName;
    struct listNode *next;
    }ListNode;

ListNode* root = NULL;

char global_root[PATH_MAX];

static void real_path(char actual_path[PATH_MAX], const char *path)
{
    strcpy(actual_path, KVFS_DATA->rootdir);
    strncat(actual_path, path, PATH_MAX);
}

static void real_path_inside_root(char actual_path[PATH_MAX], const char *path)
{
    strcpy(actual_path, KVFS_DATA->rootdir);
    strcat(actual_path,"/");
    strncat(actual_path, path, PATH_MAX);
}

int kvfs_getattr_impl(const char *path, struct stat *statbuf)
{
    int retstat;
    char actual_path[PATH_MAX],actual_path2[PATH_MAX];
    
     if(strcmp(root->hashedVal,path)==0){
      log_msg("INSIDE IF");
      path = "/";
      real_path(actual_path,path);
    }	
    else{
      log_msg("INSIDE ELSE");
      real_path_inside_root(actual_path,path);
    }
    	
    retstat = log_syscall("lstat", lstat(actual_path, statbuf), 0);
    
    log_stat(statbuf);
    
    return retstat;
}

int kvfs_readlink_impl(const char *path, char *link, size_t size)
{
    int retstat;
    char actual_path[PATH_MAX];
    real_path_inside_root(actual_path, path);
    
    retstat = log_syscall("actual_path", readlink(actual_path, link, size - 1), 0);
    if (retstat >= 0) 
    {
      link[retstat] = '\0';
      retstat = 0;
    }
    
    return retstat;
}

int kvfs_mknod_impl(const char *path, mode_t mode, dev_t dev)
{
  int retstat;
  char actual_path[PATH_MAX];
  
  real_path_inside_root(actual_path, path);

  if (S_ISREG(mode)) 
  {
     retstat = log_syscall("open", open(actual_path, O_CREAT | O_EXCL | O_WRONLY, mode), 0);
     if (retstat >= 0) 
     {
        retstat = log_syscall("close", close(retstat), 0);
     }
  } 
  else
  {
      if (S_ISFIFO(mode)) 
      {
         retstat = log_syscall("mkfifo", mkfifo(actual_path, mode), 0);
      }
      else
      {
         retstat = log_syscall("mknod", mknod(actual_path, mode, dev), 0);
      }
  }
  return retstat;
}

int kvfs_mkdir_impl(const char *path, mode_t mode)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path,path);
  return log_syscall("mkdir", mkdir(actual_path, mode), 0);
}

int kvfs_unlink_impl(const char *path)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path, path);

  return log_syscall("unlink", unlink(actual_path), 0);
}

int kvfs_rmdir_impl(const char *path)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path, path);

  return log_syscall("rmdir", rmdir(actual_path), 0);
}

int kvfs_symlink_impl(const char *path, const char *link)
{
  char flink[PATH_MAX];
  real_path_inside_root(flink, link);
  return log_syscall("symlink", symlink(path, flink), 0);
}
int kvfs_rename_impl(const char *path, const char *newpath)
{
  char actual_path[PATH_MAX];
  char fnewpath[PATH_MAX];

  real_path_inside_root(actual_path, path);
  real_path_inside_root(fnewpath, newpath);

  return log_syscall("rename", rename(actual_path, fnewpath), 0);
}

int kvfs_link_impl(const char *path, const char *newpath)
{
  char actual_path[PATH_MAX], fnewpath[PATH_MAX];
  real_path_inside_root(actual_path, path);
  real_path_inside_root(fnewpath, newpath);

  return log_syscall("link", link(actual_path, fnewpath), 0);
}

int kvfs_chmod_impl(const char *path, mode_t mode)
{
  char actual_path[PATH_MAX];
  
  real_path_inside_root(actual_path, path);

  return log_syscall("chmod", chmod(actual_path, mode), 0);
}

int kvfs_chown_impl(const char *path, uid_t uid, gid_t gid)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path, path);

  return log_syscall("chown", chown(actual_path, uid, gid), 0);
}

int kvfs_truncate_impl(const char *path, off_t newsize)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path, path);
  return log_syscall("truncate", truncate(actual_path, newsize), 0);
}

int kvfs_utime_impl(const char *path, struct utimbuf *ubuf)
{
  char actual_path[PATH_MAX];
  real_path_inside_root(actual_path, path);
  return log_syscall("utime", utime(actual_path, ubuf), 0);
}

int kvfs_open_impl(const char *path, struct fuse_file_info *fi)
{
  int retstat = 0;
  int fd;
  char actual_path[PATH_MAX];
  
  real_path_inside_root(actual_path, path);

  fd = log_syscall("open", open(actual_path, fi->flags), 0);
  if (fd < 0) 
  {
    retstat = log_error("open");
  }

  fi->fh = fd;

  log_fi(fi);
  
  return retstat;
}

int kvfs_read_impl(const char *path, char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int retstat = 0;
  
  log_fi(fi);

  return log_syscall("pread", pread(fi->fh, buf, size, offset), 0);
}

int kvfs_write_impl(const char *path, const char *buf, size_t size, off_t offset, struct fuse_file_info *fi)
{
  int retstat = 0;
  
  log_fi(fi);

  return log_syscall("pwrite", pwrite(fi->fh, buf, size, offset), 0);
}

int kvfs_statfs_impl(const char *path, struct statvfs *statv)
{
  int retstat = 0;
  char actual_path[PATH_MAX];

   if(strcmp(root->hashedVal,path)==0){
      path = "/";
      real_path(actual_path,path);
    }	
    else{
      real_path_inside_root(actual_path,path);
    }
  retstat = log_syscall("statvfs", statvfs(actual_path, statv), 0);
  
  log_statvfs(statv);
  
  return retstat;
}

int kvfs_flush_impl(const char *path, struct fuse_file_info *fi)
{
  log_fi(fi);
  return 0;
}

int kvfs_release_impl(const char *path, struct fuse_file_info *fi)
{
  log_fi(fi);

  return log_syscall("close", close(fi->fh), 0);
}

int kvfs_fsync_impl(const char *path, int datasync, struct fuse_file_info *fi)
{
  log_fi(fi);
#ifdef HAVE_FDATASYNC
  if (datasync)
  {
    return log_syscall("fdatasync", fdatasync(fi->fh), 0);
    else
  }
#endif  
  return log_syscall("fsync", fsync(fi->fh), 0);
}

#ifdef HAVE_SYS_XATTR_H
int kvfs_setxattr_impl(const char *path, const char *name, const char *value, size_t size, int flags)
{
  char actual_path[PATH_MAX];
  real_path(actual_path, path);

  return log_syscall("lsetxattr", lsetxattr(actual_path, name, value, size, flags), 0);
}

int kvfs_getxattr_impl(const char *path, const char *name, char *value, size_t size)
{
  int retstat = 0;
  char actual_path[PATH_MAX];
  
  real_path(actual_path, path);

  retstat = log_syscall("lgetxattr", lgetxattr(actual_path, name, value, size), 0);
  
  return retstat;
}

int kvfs_listxattr_impl(const char *path, char *list, size_t size)
{
  int retstat = 0;
  char actual_path[PATH_MAX];
  char *ptr;
  
  real_path(actual_path, path);

  retstat = log_syscall("llistxattr", llistxattr(actual_path, list, size), 0);
 
  return retstat;
}

int kvfs_removexattr_impl(const char *path, const char *name)
{
  char actual_path[PATH_MAX];
  real_path(actual_path, path);

  return log_syscall("lremovexattr", lremovexattr(actual_path, name), 0);
}
#endif

int kvfs_opendir_impl(const char *path, struct fuse_file_info *fi)
{
  DIR *dp;
  int retstat = 0;
  char actual_path[PATH_MAX];
   if(strcmp(root->hashedVal,path)==0){
      path = "/";
      real_path(actual_path,path);
    }	
    else{
     real_path_inside_root(actual_path,path);
    }
  dp = opendir(actual_path);
  if (dp == NULL)
  {
    retstat = log_error("opendir");
  }
  fi->fh = (intptr_t) dp;
  
  log_fi(fi);
  
  return retstat;
}

int kvfs_readdir_impl(const char *path, void *buf, fuse_fill_dir_t filler, off_t offset, struct fuse_file_info *fi)
{
    int retstat = 0;
    DIR *dp;
    struct dirent *de;
    dp = (DIR *) (uintptr_t) fi->fh;

    de = readdir(dp);
    if (de == 0) 
    {
      retstat = log_error("readdir");
      return retstat;
    }
    do 
    {
      if (filler(buf, de->d_name, NULL, 0) != 0) 
      {
        return -ENOMEM;
      }
    } while ((de = readdir(dp)) != NULL);
    
    log_fi(fi);
    
    return retstat;
}

int kvfs_releasedir_impl(const char *path, struct fuse_file_info *fi)
{
  int retstat = 0;
  log_fi(fi);
  
  closedir((DIR *) (uintptr_t) fi->fh);
  
  return retstat;
}

int kvfs_fsyncdir_impl(const char *path, int datasync, struct fuse_file_info *fi)
{
  int retstat = 0;
  
  log_fi(fi);
  
  return retstat;
}

int kvfs_access_impl(const char *path, int mask)
{
  int retstat = 0;
  char actual_path[PATH_MAX],actual_path2[PATH_MAX];
  
  if(first_run==0)
  {
  
    //Predefine the root value to an easily accessible name.
    root = (ListNode *)malloc(sizeof(ListNode));
    root->fileName = "/";
    root->hashedVal = str2md5(root->fileName,strlen(root->fileName));

   first_run=1;
  }
   if(strcmp(root->hashedVal,path)==0){
      path = "/";
      real_path(actual_path,path);
    }	
    else{
      real_path_inside_root(actual_path,path);
    }
  retstat = access(actual_path, mask);
  
  if (retstat < 0)
  {
    retstat = log_error("access");
  }
  return retstat;
}
int kvfs_ftruncate_impl(const char *path, off_t offset, struct fuse_file_info *fi)
{
  int retstat = 0;
  
  log_fi(fi);
  
  retstat = ftruncate(fi->fh, offset);
  if (retstat < 0)
  {
    retstat = log_error("ftruncate");
  }
  
  return retstat;
}

int kvfs_fgetattr_impl(const char *path, struct stat *statbuf, struct fuse_file_info *fi)
{
    int retstat = 0;
    
    log_fi(fi);

    if (!strcmp(path, "/"))
    {
      return kvfs_getattr_impl(path, statbuf);
    }
    retstat = fstat(fi->fh, statbuf);
    if (retstat < 0)
    {
      retstat = log_error("fstat");
    }

    log_stat(statbuf);
    
    return retstat;
}
