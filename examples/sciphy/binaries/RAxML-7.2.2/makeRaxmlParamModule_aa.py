#! /usr/bin/env python
#to execute: python execute_raxml.py /data/home/aluno/Desktop/protozoa-jan-2011/exp_original_04_03/output/teste ii.phylip /data/home/aluno/Desktop/protozoa-jan-2011/exp_original_04_03/output/1/modelo_de_mg.txt 2 4

import os, re

def paramModuleRaxml(dirin, phy, model, bootstrap, nb_categ):
  phylip = str(phy)
  mod = str(model)
  btp = str(bootstrap)
  nbc = str(nb_categ)
  model = 'PROTGAMMA' + mod

  # Trabalhando com o diretorio script onde esta o raxml.conf 
  for f in os.listdir(dirin):
    if f.endswith('.phylip'):                                                    
      phylip = f

      # Executando Parametros do Raxml
      # 1.- RAXML: Estimating a Single Maximum-Likelihood Tree from Protein Sequences
      os.chdir(dirin)
      retval = os.getcwd()
      print "Current working directory %s" % retval
      
      #cmd = "/bin/" by Thaylon
      cmd = ""

      cmd_raxml_s = cmd + "raxmlHPC -s " + phylip + " -n " + phylip + "_raxml_tree1.singleTree -c " + nbc + " -f d -m " + model + " -p 1"
      print "raxmlHPC -s " + phylip + " -n " + phylip + "_raxml_tree1.singleTree -c " + nbc + " -f d -m " + model + " -p 1"
      handle_s = os.popen(cmd_raxml_s, 'r', 1)
      for line_s in handle_s:
        print line_s,
      handle_s.close()

      # 2.- RAXML: Estimating a Set of Non-Parametric Bootstrap Trees
      cmd_raxml_b = cmd + "raxmlHPC -s " + phylip + " -n " + phylip + "_tree2.raxml -c " + nbc + " -f d -m " + model + " -b 234534251 -N " + btp + " -p 1"
      print "raxmlHPC -s " + phylip + " -n " + phylip + "_tree2.raxml -c " + nbc + " -f d -m " + model + " -b 234534251 -N " + btp + " -p 1"
      handle_b = os.popen(cmd_raxml_b, 'r', 1)
      for line_b in handle_b:
        print line_b,
      handle_b.close()

      # 3.- RAXML: Projecting Bootstrap Confidence Values onto ML Tree
      cmd_raxml_c = cmd + "raxmlHPC -f b -m " + model + " -c " + nbc + " -s " + phylip + " -z " + "RAxML_bootstrap." + phylip + "_tree2.raxml -t RAxML_bestTree." + phylip + "_raxml_tree1.singleTree -n " + phylip + "_tree3.BS_TREE -p 1"
      print "raxmlHPC -f b -m " + model + " -c " + nbc + " -s " + phylip + " -z " + "RAxML_bootstrap." + phylip + "_tree2.raxml -t RAxML_bestTree." + phylip + "_raxml_tree1.singleTree -n " + phylip + "_tree3.BS_TREE -p 1"
      handle_c = os.popen(cmd_raxml_c, 'r', 1)
      for line_c in handle_c:
        print line_c,
      handle_c.close()
  
  
#paramModuleRaxml(dirin, phy, mod, bootstrap, nb_categ)

