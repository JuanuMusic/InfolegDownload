import os
import re
import urllib
import pandas as pd
import html2text
import requests
from bs4 import BeautifulSoup
import io

class Download(object):
    
    text_dir = './DATA/text/'
    dataframe = None

    def __init__(self, dataset_path, text_dir=text_dir):
        print('Loading data from',dataset_path,'...')
        self.dataframe = self.__get_dataframe(dataset_path)
        return

    def __get_dataframe(self,dataset_path):
        df_retval = pd.read_csv(dataset_path)

        #   Reemplazo los valores NaN de Texto Original con la palabra EMPTY
        df_retval['texto_original'] = df_retval['texto_original'].fillna('EMPTY').astype(str)

        # Genero variables extras
        df_retval['is_valid_url'] = df_retval.apply(lambda row: self.__is_valid_url(row['texto_original']), axis=1)
        df_retval['local_path'] = df_retval.apply(lambda row: os.path.join(self.__get_path_from_url(row['texto_original']), str(row['id_norma']) + '.txt'), axis=1)

        return df_retval

    def __get_relative_path_from_url(self, url):
        # Obtengo el path para separar por carpetas, usando el mismo valor de la URL
        start = url.find('anexos/') + len('anexos/')
        if(start < 0):
            return url
        
        end = start+url[start:].find('/')
        if(end < 0):
            return url

        if(len(url) >= start and len(url) >= end):
            return url[start:end]
        else:
            return url

    def __get_path_from_url(self, url):

        relative_path = self.__get_relative_path_from_url(url)
        dir_path = os.path.join(self.text_dir,relative_path)
        return dir_path

    def __get_filtered_dataframe(self):

        print('Filtering existing files from the dataframe...')
        return self.dataframe.loc[self.dataframe.apply(lambda row: row['is_valid_url'] and (not os.path.isfile(row['local_path'])), axis=1)]

    def start_download(self):

        # Get Missing Files
        filtered_dataframe = self.__get_filtered_dataframe()
        
        print('*'*20)
        print(len(filtered_dataframe),'Documentos a descargar')
        print('*'*20)

        last_encoding = ''
        for i, row in filtered_dataframe.iterrows():
        
            url = row['texto_original']
            
            if(url is not 'EMPTY' and self.__is_valid_url(url)):
                
                # Obtengo el path para separar por carpetas, usando el mismo valor de la URL
                text_path = row['local_path']
                directory = text_path.replace(str(row['id_norma']) + '.txt','')

                current_encoding = ''

                try:
                
                    # Creo la carpeta si no existe.
                    if(not os.path.exists(directory)):
                        print('Creando directorio',directory,'...')
                        os.makedirs(directory)

                    # Obtengo la pag
                    r = requests.get(url)
                    current_encoding = r.encoding
                    page_text = r.text
                
                    if(r.encoding != last_encoding):
                        print('Current encoding',r.encoding,'...')
                        last_encoding = r.encoding


                    # HTML to text
                    soup = BeautifulSoup(page_text,'lxml')
                    [s.extract() for s in soup('script')] # Remove script tags.
                    texto_list = [e.text for e in soup.find_all() if e.text.strip() != '' else ''] # Get the text from all the elements on the HTML
                    texto = '\n'.join(texto_list)
                    try:
                        self.save_text_to_file(texto, text_path, r.encoding)
                    except UnicodeEncodeError as encodeEx:
                        print('Forcing utf-8 for file',text_path,' from ',url,'...')
                        self.save_text_to_file(texto, text_path, 'utf-8')
                        print('*'*64)

                except Exception as ex:
                    print('EXCEPTION:')
                    print(ex)
                    print('text_path', text_path)
                    print('url', url)
                    print('current_encoding',current_encoding)
                    print('*'*64)
                    input('Press a key to continue...')

        print('*'*20)
        print ('Descarga Finalizada')
        print('*'*20)

    def save_text_to_file(self,text, path, encoding):
        with io.open(path, 'w', encoding=encoding) as f:
            f.write(text)
    
    def __is_valid_url(self, value):
        regex = re.compile(
                r'^(?:http|ftp)s?://' # http:// or https://
                r'(?:(?:[A-Z0-9](?:[A-Z0-9-]{0,61}[A-Z0-9])?\.)+(?:[A-Z]{2,6}\.?|[A-Z0-9-]{2,}\.?)|' #domain...
                r'localhost|' #localhost...
                r'\d{1,3}\.\d{1,3}\.\d{1,3}\.\d{1,3})' # ...or ip
                r'(?::\d+)?' # optional port
                r'(?:/?|[/?]\S+)$', re.IGNORECASE)

        return re.match(regex, value) is not None


if __name__ == '__main__':

    DATA_PATH = './DATA/'
    
    # Cargo los datos del CSV
    dataset_path = os.path.join(DATA_PATH, 'base-infoleg-normativa-nacional.csv')
    
    if(not os.path.isfile(dataset_path)):
        print('La ruta',dataset_path,'no existe o no es valida.')
        exit()
    
    download = Download(dataset_path)
    download.start_download()

    

    
    
