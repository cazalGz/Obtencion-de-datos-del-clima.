import requests
import pandas as pd
from datetime import datetime, timedelta
from sqlalchemy import create_engine, Column, Integer, String, DateTime, inspect
from sqlalchemy.orm import sessionmaker, declarative_base
import os
from user import user, password,host,database_name, api_key, output_folder



#Crea una conexión a la base de datos:
DATABASE_URL = f'mysql+mysqlconnector://{user}:{password}@{host}/{database_name}'
engine = create_engine(DATABASE_URL)

#Define una clase para la tabla:
Base = declarative_base()
class Clima(Base):
    __tablename__ = 'clima'
    id = Column(Integer, primary_key=True, autoincrement=True)
    ciudad = Column(String(50))
    hora_tomada = Column(DateTime)
    temperatura = Column(String(50))
    sensación_térmica=Column(String(50))
    info=Column(String(50))
    presión=Column(Integer)
    humedad=Column(Integer)

#Crea la tabla:
inspector = inspect(engine)
if not inspector.has_table('clima'):
    # La tabla no existe, así que la creamos
    Base.metadata.create_all(engine)
    print("Tabla 'clima' creada.")
else:
    print("La tabla 'clima' ya existe.")





BASE_URL ='https://api.openweathermap.org/data/3.0/onecall'

cityList = ["London", "New York", "Cordoba", "Buenos Aires",  "Bogota", "Tokio"]
coordList = ["lat=31&lon=64", "lat=40&lon=-73", "lat=-31&lon=-64", "lat=-34&lon=-58", "lat=4&lon=74", "lat=35&lon=139"]


def Obtencion_de_clima(city, coords):
    url =  f'{BASE_URL}?{coords}&appid={api_key}&units=metric'

    response = requests.get(url)
    if response.status_code == 200:
        response_json = response.json()
        datos_normalizados=pd.json_normalize(response_json)
        print("datos obtenidos")    
    else:
        print("error de obtención de datos")
        print(response.status_code)

    #modificando las unidades de los datos
    datos_normalizados['current.dt'] = pd.to_datetime(datos_normalizados['current.dt'], unit='s')

    #agregar los datos del dataframe en las columnas correspondientes de la tabla 
    ciudad=city
    temperatura = int(datos_normalizados['current.temp'].iloc[0])
    hora_tomada = datos_normalizados['current.dt'][0]
    sensación_térmica=int(datos_normalizados['current.feels_like'].iloc[0])
    info=datos_normalizados['current.weather'][0][0]['description']
    presión=int(datos_normalizados['current.pressure'].iloc[0])
    humedad=int(datos_normalizados['current.humidity'].iloc[0])


    #Crea una sesión:
    Session = sessionmaker(bind=engine)
    session = Session()

    # Crear una instancia de la clase Clima y agregarla a la sesión
    datos_clima = Clima(ciudad=ciudad, temperatura=temperatura, hora_tomada=hora_tomada, sensación_térmica=sensación_térmica ,info=info, presión=presión, humedad=humedad)
    session.add(datos_clima)
    session.commit()


    cont = 1
    while cont<3:
        # Crear un objeto timedelta de un día
    
        un_dia = timedelta(days=cont)
        # Obtener la fecha actual
        fecha_hora_actual = datetime.now()
        # Restar un día a la fecha actual
        fecha_hora_ayer = fecha_hora_actual - un_dia
        fecha_ayer_unix=int(fecha_hora_ayer.timestamp())

        url2 = f'{BASE_URL}/timemachine?{coords}&dt={fecha_ayer_unix}&appid={api_key}&units=metric'

        response = requests.get(url2)
        if response.status_code == 200:
            response_json = response.json()
            datos_dias_atras_normalizados=pd.json_normalize(response_json)    
        else:
            print("error de obtención de datos")
            print(response.status_code)
    

        #modificando las unidades de los datos
        datos_dias_atras_normalizados['data'][0][0]['dt'] = pd.to_datetime(datos_dias_atras_normalizados['data'][0][0]['dt'], unit='s')

        #agregar los datos del dataframe en las columnas correspondientes de la tabla 

        ciudad=city
        temperatura = int(datos_dias_atras_normalizados['data'][0][0]['temp'])
        sensación_térmica= int(datos_dias_atras_normalizados['data'][0][0]['feels_like'])
        hora_tomada = datos_dias_atras_normalizados['data'][0][0]['dt']
        info= datos_dias_atras_normalizados['data'][0][0]['weather'][0]['description']
        presión=int(datos_dias_atras_normalizados['data'][0][0]['pressure'])
        humedad=int(datos_dias_atras_normalizados['data'][0][0]['humidity'])


    

        # Crear una instancia de la clase Clima y agregarla a la sesión
        datos_clima = Clima(ciudad=ciudad, temperatura=temperatura, hora_tomada=hora_tomada, sensación_térmica=sensación_térmica ,info=info, presión=presión, humedad=humedad)
        session.add(datos_clima)
        session.commit()
        cont+=1

    #hacer la query a la base de datos y descargarlo como archivo csv
    query=f'SELECT * FROM clima WHERE ciudad="{city}"'
    datos_guardados = pd.read_sql_query(query, engine)
    print(f"Datos meteorológicos de {city} almacenados con éxito.")
    csv_filename = os.path.join(output_folder, f'clima{city}.csv')
    datos_guardados.to_csv(csv_filename, index=False, encoding='utf-8')



for city, coords in zip(cityList, coordList):
    Obtencion_de_clima(city, coords)

