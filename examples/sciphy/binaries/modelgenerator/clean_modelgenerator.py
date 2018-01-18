#! /usr/bin/env python
# to execute: python clean_modelgenerator.py /data/home/aluno/Desktop/protozoa-jan-2011/exp_original_04_03/output/1 i.mg

import sys, os, re
import shutil as sh

mg_file = sys.argv[1]

def paramCleanModelgenerator(mg_file):
  # Abrindo o modelgenerator para leitura
  os.chmod(mg_file, 0755)  # Assume it's a file
  # Lendo o arquivo modelgenerator0.out para editar e obter parametros AIC1, AIC2 ou BIC
  text_mg = open(mg_file).read()
  
  # Extraindo informacao de modelgenerator0.out 
  mg_parameter = text_mg.split("\n\n****Akaike Information Criterion 1 (AIC1)****\n\n")
  mg_info = mg_parameter[1].split(":")                                          # mg_info[1] = modelo e parametros limpos
  parametro_filogenia = mg_info[1].split("\n")                                  # param Mod. Evol. = MODEL+I+G+F (3 ult. sem ordem)
  parametro_filogenia_clean = parametro_filogenia[0].replace(" ", "")           # Limpando de espacos em branco
  modelo_param = parametro_filogenia_clean.split("+")                            
  model_only = str(modelo_param[0])

  # Criando o arquivo modelo evolutivo
  model_file = open(mg_file + ".modelFromMG.txt","w")
  #print model_file
  conteudo_texto = model_file.write(model_only)
  model_file.close()

paramCleanModelgenerator(mg_file)

