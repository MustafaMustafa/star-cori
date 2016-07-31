/*
 * This macro is to check the intergrity of prorductions.
 *
 * File:   checkProduction.C
 * Author: 
 *         **Mustafa Mustafa (mmustafa@lbl.gov)
 *
 * **Code Maintainer
 */

#include <fstream>
#include <iostream>

#include "TString.h"
#include "TFile.h"
#include "TTree.h"
#include "TUnixSystem.h"


using namespace std;

enum fileState {Zombie = -1, NoTree = -2, CannotOpenFile = -3};

void deleteFile(TString filename);
int getNumberOfEvents(TString filename, TString treeName);

void checkProduction(TString muFileName, int const nEvents, float const minEventsRatio = 0.95)
{
   TString logFileName = muFileName;
   logFileName = logFileName.ReplaceAll(".MuDst.root", ".nEventsCheck.yaml");

   ofstream logOs(logFileName.Data(),ios::out);

   logOs << '\n';
   logOs << "nEvents: " << nEvents << endl;
   logOs << "MuDstName: " << muFileName << endl;
   logOs << '\n';

   int nMuDstEvents = getNumberOfEvents(muFileName, "MuDst");

   logOs << "nMuDstEvents: " << nMuDstEvents << endl;

   if (nMuDstEvents > 0)
   {
     if (static_cast<float>(nMuDstEvents) / nEvents < minEventsRatio)
     {
       logOs << '\n';
       logOs << "Status: below_threshold" << endl;
       deleteFile(muFileName);
       return;
     }
     else
     {
       logOs << "Status: good" << endl;
     }
   }
   else if (nMuDstEvents == 0)
   {
      logOs << "Status: empty" << endl;
   }
   else
   {
      logOs << "Status: corrupted" << endl;
      deleteFile(muFileName);
      exit(1);
   }

   return;
}

void deleteFile(TString filename)
{
   TString command = "rm -f " + filename;
   gSystem->Exec(command.Data());
}

int getNumberOfEvents(TString filename, TString treeName)
{
   TFile* file = TFile::Open(filename.Data());

   if (!file)
   {
      cerr << '\n';
      cout << "WARNING - CANNOT OPEN FILE: " << filename << endl;
      return CannotOpenFile;

   }

   // Check if the root file is zombie
   if (file->IsZombie())
   {
      cout << "WARNING - ZMOBIE FILE: " << filename << endl;
      file->Close();
      return Zombie;
   }

   // Get the number of events stored in the etree
   TTree* tree = (TTree*)file->Get(treeName.Data());

   if (tree)
   {
      int nEvents = tree->GetEntries();
      file->Close();
      return nEvents;
   }
   else
   {
      cout << "WARNING - TREE DOES NOT EXIST: " << filename << endl;
      file->Close();
      return NoTree;
   }
}
