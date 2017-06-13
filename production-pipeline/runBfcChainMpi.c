/* Author : Mustafa Mustafa <mmustafa@lbl.gov> */
#include <mpi.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
 
void getBaseFileName(char* fileName, char* baseName);
void getMuDstFileName(char* baseName, char* outFileName);
void getOutFileName(char* baseName, char* outFileName, int rank, int starEvt, int endEvt);
void getPicoFileName(char* baseName, char* outFileName);
void getPicoOutFileName(char* baseName, char* outFileName, int const rank, int const startEvt, int const endEvt);

#define FileNameArraySize 200

int main (int argc, char* argv[])
{
  char daqFileName[FileNameArraySize] = "";
  char chain[2000] = "";
  int firstEvent = 1;
  int lastEvent = 1;

  if(argc != 5)
  {
    printf("Incorrect number of parameters.\n");
    printf("Usage: ./runBfcChainMpi.o firstEvent lastEvent chain daqFile.daq\n");
    exit(1);
  }
  else
  {
    firstEvent  = atoi(argv[1]);
    lastEvent   = atoi(argv[2]);
    strcpy(chain, argv[3]);
    strcpy(daqFileName, argv[4]);
  }

  int rank, nProcesses;
  MPI_Init (&argc, &argv);                      /* starts MPI */
  MPI_Comm_rank (MPI_COMM_WORLD, &rank);        /* get current process id */
  MPI_Comm_size (MPI_COMM_WORLD, &nProcesses);  /* get number of processes */

  // sleep to allow first rank to invoke the DB to cache
  if(rank > 0)
  {
    sleep(60+5*rank);
  }

  int eventsPerProcess = (int)((double)(lastEvent-firstEvent)/nProcesses + 0.5);
  int firstEventThisProcess = 1 + eventsPerProcess * rank;
  int lastEventThisProcess  = (rank + 1) * eventsPerProcess;

  if(rank == nProcesses - 1)
  {
    lastEventThisProcess = lastEvent;
  }

  char baseName[FileNameArraySize] = "";
  char outFileName[FileNameArraySize] = "";
  getBaseFileName(daqFileName, baseName);
  getOutFileName(baseName, outFileName, rank, firstEventThisProcess, lastEventThisProcess);

  printf("%-20s = %s\n", "Daq file", daqFileName);
  printf("%-20s = %-10d\n", "Number of processes", nProcesses);
  printf("%-20s = %-10d\n", "Process id", rank);
  printf("%-20s = %d\n", "Start event", firstEventThisProcess);
  printf("%-20s = %d\n", "End event", lastEventThisProcess);
  printf("%-20s = %s\n", "Out file name", outFileName);

  // change directory for this process
  char pDir[50] = "";
  snprintf(pDir, sizeof(pDir), "process_%d", rank);
  char command[3000] = "";
  snprintf(command, sizeof(command), "mkdir %s", pDir);
  system(command);
  chdir(pDir);
  snprintf(command, sizeof(command), "cp ../runBfcChainMpi.o .");
  system(command);

  // run BFC chain
  snprintf(command, sizeof(command), "/bin/tcsh -c \"source /usr/local/star/group/templates/cshrc;root4star -l -b -q -x 'bfc.C(%d, %d, \\\"%s\\\", \\\"%s\\\")'\"", firstEventThisProcess, lastEventThisProcess, chain, daqFileName);
  system(command);

  // change MuDst file name
  char muDstFileName[FileNameArraySize];
  getMuDstFileName(baseName, muDstFileName);
  snprintf(command, sizeof(command), "mv %s ../%s", muDstFileName, outFileName);
  system(command);

  char picoFileName[FileNameArraySize];
  getPicoFileName(baseName, picoFileName);
  getPicoOutFileName(baseName, outFileName, rank, firstEventThisProcess, lastEventThisProcess);
  snprintf(command, sizeof(command), "mv %s ../%s", picoFileName, outFileName);
  system(command);

  MPI_Finalize();
  printf("MPI_Finalize called.\n");
  return 0;
}

void getBaseFileName(char* fileName, char* baseName)
{
  char* begin = strstr(fileName, "st_");
  char* end   = strstr(fileName, ".daq");
  strncpy(baseName, begin, end - begin);
}

void getMuDstFileName(char* baseName, char* outFileName)
{
  getOutFileName(baseName, outFileName, -1, -1, -1);
}

void getOutFileName(char* baseName, char* outFileName, int const rank, int const startEvt, int const endEvt)
{
  if(startEvt == -1)
  {
    snprintf(outFileName, FileNameArraySize, "%s.MuDst.root", baseName);
  }
  else
  {
    snprintf(outFileName, FileNameArraySize, "%s.%d.SubEvts_%d_%d.MuDst.root", baseName, rank, startEvt, endEvt);
  }
}
//-------------------------------
void getPicoFileName(char* baseName, char* outFileName)
{
  getPicoOutFileName(baseName, outFileName, -1, -1, -1);
}

void getPicoOutFileName(char* baseName, char* outFileName, int const rank, int const startEvt, int const endEvt)
{
  if(startEvt == -1)
  {
    snprintf(outFileName, FileNameArraySize, "%s.picoDst.root", baseName);
  }
  else
  {
    snprintf(outFileName, FileNameArraySize, "%s.%d.SubEvts_%d_%d.picoDst.root", baseName, rank, startEvt, endEvt);
  }
}
