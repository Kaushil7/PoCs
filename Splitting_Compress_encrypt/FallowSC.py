# Load libraries-
from azure.storage.blob import BlobServiceClient, BlobClient, ContainerClient
import logging
from datetime import datetime
from time import *
import os
import cmd
import gnupg
import base64
from azure.keyvault.secrets import SecretClient
from azure.identity import ManagedIdentityCredential, ClientSecretCredential
from pprint import pprint
import os, uuid
import params
import sys
import zipfile


#Reading all the paramater values from the params file
Param_value_vault_url = params.vault_url
Param_value_GPG_Secret_Passphrase_KVSecret_Name = params.GPG_Secret_Passphrase_KVSecret_Name

#change private to public(name)
Param_value_GPG_Public_Key_KVSecret_Name = params.GPG_Public_Key_KVSecret_Name

#importing the recipient email id
Param_value_GPG_Public_Key_recipient_email = params.GPG_Public_Key_recipient_email



Param_value_Input_Storage_Account_url = params.Input_Storage_Account_url
Param_value_Input_Storage_Container_Name = params.Input_Storage_Container_Name
Param_Value_Input_Storage_Folder_Path = params.Input_Storage_Folder_Path
Param_value_Output_Storage_Account_url = params.Output_Storage_Account_url
Param_value_Output_Storage_Container_Name = params.Output_Storage_Container_Name


#Reading Input file name from command line
input_blob_file_name = sys.argv[1]

    
input_blob_name = Param_Value_Input_Storage_Folder_Path + input_blob_file_name


#Setting up Credential for batch pool user assigned Managed Identity
uami_credential = ManagedIdentityCredential()
kv_client = SecretClient(vault_url=Param_value_vault_url, credential=uami_credential)


gpg = gnupg.GPG(gnupghome='/tmp/gnupg',verbose=True)

os.system('GNUPGHOME="${GNUPGHOME:-$HOME/.gnupg}" gpgconf --kill gpg-agent')

os.system('gpg-agent --homedir "/tmp/gnupg" --daemon')

# Read GPG Secret Passphrase from KeyVault(change the passphrase variable)
pzdbsecretpassphrase = kv_client.get_secret(Param_value_GPG_Secret_Passphrase_KVSecret_Name).value


# Read GPG Public Key from KeyVault(to change)
pzdbsecretb64 = kv_client.get_secret(Param_value_GPG_Public_Key_KVSecret_Name).value
pzdbsecret = base64.b64decode(pzdbsecretb64).decode('ascii')

#Read GPG Recipient email ID from keyvault
recipient_email = kv_client.get_secret(Param_value_GPG_Public_Key_recipient_email).value



# Establish connection with the blob storage account
input_blob_service_client = BlobServiceClient(account_url=Param_value_Input_Storage_Account_url, credential=uami_credential)

output_blob_service_client = BlobServiceClient(account_url=Param_value_Output_Storage_Account_url, credential=uami_credential)

input_container_client=input_blob_service_client.get_container_client(Param_value_Input_Storage_Container_Name)

output_container_client=output_blob_service_client.get_container_client(Param_value_Output_Storage_Container_Name)  

#input file name
inputblobclient = input_container_client.get_blob_client(input_blob_name)

  

#cleanup
if os.path.exists('split_out'):
    dir_list = os.listdir('split_out')
    for x in dir_list:
        del_blob = 'split_out/'+x
        os.remove(del_blob)

with open(input_blob_file_name, "wb") as my_blob:
    blob_data = inputblobclient.download_blob()
    blob_data.readinto(my_blob)		

#Add here new code
def split_csv(input_file, output_directory, chunk_size):
    if not os.path.exists(output_directory):
        os.makedirs(output_directory)

    owd = os.getcwd()  

    with open(input_file, 'r') as f:
        header = f.readline()
        chunk = [header]
        chunk_size_bytes = len(header)

        for line in f:
            chunk.append(line)
            chunk_size_bytes += len(line)

            if chunk_size_bytes >= chunk_size * 1024 * 1024:
                output_file = os.path.join(output_directory, f'chunk_{len(os.listdir(output_directory)) + 1}')                
                with open(output_file, 'w') as chunk_file:
                    chunk_file.writelines(chunk)
                    
                chunk = [header]
                chunk_size_bytes = len(header)
                curfile= f'chunk_{len(os.listdir(output_directory))}'
                #with zipfile.ZipFile(curfile[:-4]+'.zip',mode='w') as zipped:
                   # zipped.write(curfile)

        if len(chunk) > 1:
            output_file = os.path.join(output_directory, f'chunk_{len(os.listdir(output_directory)) + 1}')
            with open(output_file, 'w') as chunk_file:
                chunk_file.writelines(chunk)

    no_files = len(os.listdir(output_directory))
    os.chdir(output_directory+'/')
    for x in range(1,no_files+1):
        dir = 'chunk_'+str(x)
        outf = 'chunk_'+ str(x) + '.zip'
        with zipfile.ZipFile(dir+'.zip',mode='w', compression=zipfile.ZIP_DEFLATED) as zipped:
                zipped.write(dir)         
        with open(outf, "rb") as data:                	        
                outputblobclient = output_container_client.get_blob_client(outf)
                outputblobclient.upload_blob(data,overwrite=True)
        if os.path.exists(outf):
                os.remove(outf)
        else:
                print("The " + outf + '.zip'+ "file does not exist")
        if os.path.exists(dir):
                os.remove(dir)
        else:
                print("The " + dir + '.zip'+ "file does not exist")                 
    #encrypt...   
    
    os.chdir(owd)         

#calling function here 
chunk_size_mb = 50
split_csv(input_blob_file_name,'split_out',chunk_size_mb)
print('CSV file split complete.')


#zipping before encryption
#with zipfile.ZipFile(input_blob_file_name+ '.zip',mode='w') as zipped:
    #zipped.write(input_blob_file_name)
    

#import_gpg_key_result = gpg.import_keys(pzdbsecret, passphrase= pzdbsecretpassphrase)
#key_id=import_gpg_key_result.fingerprints[0]
#gpg.trust_keys(key_id, 'TRUST_ULTIMATE')
#pprint(import_gpg_key_result.results)


#public_keys = gpg.list_keys()
#pprint(public_keys)

# Apply GPG Encryption (recipient should be an email)
#with open(input_blob_file_name + '.zip', 'rb') as f:
  #status = gpg.encrypt_file(f,recipients=[recipient_email],output=local_encrypted_file_name)
    

#with open(local_encrypted_file_name, "rb") as data:
    #outputblobclient.upload_blob(data,overwrite=True)

if os.path.exists(input_blob_file_name):
  os.remove(input_blob_file_name)
else:
  print("The " + input_blob_file_name + "file does not exist")	



#cleanup
if os.path.exists('split_out'):
    dir_list = os.listdir('split_out')
    for x in dir_list:
        del_blob = 'split_out/'+x
        os.remove(del_blob)
if os.path.exists('split_out'):
    os.rmdir('split_out')

blob_client_delete = input_blob_service_client.get_blob_client(container=Param_value_Input_Storage_Container_Name, blob=input_blob_file_name)
blob_client_delete.delete_blob()

#print (status.ok)
#print (status.status)
#print (status.stderr)
