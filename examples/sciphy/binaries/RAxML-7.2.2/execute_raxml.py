#! /usr/bin/env python
import sys, os, re
import shutil as sh

dirin_current = sys.argv[1]
phylip_file = sys.argv[2]
mg_file = sys.argv[3]
bootstrap = sys.argv[4]
nbcat = sys.argv[5]

def paramExecuteRaxml(dirin_current, phylip_file, mg_file, bootstrap, nbcat):
  # Abrindo o modelgenerator para leitura
  os.chmod(mg_file, 0755)  # Assume it's a file

  # Lendo o arquivo modelo.txt para ler o modelo
  text_mg = open(mg_file).read()

  # Definindo variaveis
  btp = str(bootstrap)
  mod = str(text_mg)
  nbc = str(nbcat)
  mydict_modelRAXML = ['DAYHOFF', 'DCMUT', 'JTT', 'MTREV', 'WAG', 'RTREV', 'CPREV', 'VT', 'BLOSUM62', 'MTMAM', 'MTART', 'MTZOA', 'LG', 'PMB', 'HIVB', 'HIVW', 'JTTDCMUT', 'FLU','GTR']                                                #a versao VI e usado HKY85

  #1.- RAXML: Se o modelo RAXML bate com MG
  found = False
  for x in mydict_modelRAXML[:]:
    m = re.match(mod + "$", x, re.IGNORECASE)
    if m:
      found  = True
      modelHit = repr(x).split("'");
#      print "param_model_evol => ", mod, ", modelPHYML => ", modelHit[1]        
      # Gravando em um arquivo o file do MG encontrado no Raxml
      model_file = open(phylip_file + ".modelMGBateRaxml.txt","w")
      conteudo_texto = model_file.write(modelHit[1])
      model_file.close()
      # chamando o script para executar raxml
      import makeRaxmlParamModule_aa
      makeRaxmlParamModule_aa.paramModuleRaxml(dirin_current, phylip_file, modelHit[1], btp, nbc)
      #break

  #2.- RAXML: Se o modelo RAXML nao bate com MG, usando o modelo JC default de RAXML
  if found is False:
#    print "  No hit: Executing Raxml using default modelRaxml =>" + " " + mydict_modelRAXML[0] +"\n"
    # Gravando em um arquivo o file do MG encontrado no Raxml
    model_file = open(phylip_file + ".modelSORaxml.txt","w")
    conteudo_texto = model_file.write(mydict_modelRAXML[0])
    model_file.close()
    # chamando o script para executar raxml
    import makeRaxmlParamModule_aa
    makeRaxmlParamModule_aa.paramModuleRaxml(dirin_current, phylip_file, mydict_modelRAXML[0], btp, nbc)

 ### model_file = open("modelo.txt","w")
 ### conteudo_texto = model_file.write(model_only)
 ### model_file.close()

paramExecuteRaxml(dirin_current, phylip_file, mg_file, bootstrap, nbcat)
