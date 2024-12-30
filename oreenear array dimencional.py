# data = [
# [2015, 'Extremadamente problemático/ No puede hacerlo', 8320], [2016, 'Extremadamente problemático/ No puede hacerlo', 8686], [2019, 'Levemente problemático', 249557], [2019, 'Moderadamente  problemático', 106014],  [2015, 'No sabe', 34771], [2016, 'No sabe', 32161], [2017, 'No sabe', 31643], [2018, 'No sabe', 30743], [2019, 'No sabe', 31428], [2019, 'Severamente problemático', 26942], [2020, 'Severamente problemático', 35306]
# ]

# # Diccionario que define el orden personalizado
# priority_order = {
#         'Nada problemático': 1,
#         'Levemente problemático': 2,
#         'Moderadamente  problemático' :3,
#         'Severamente problemático' : 4,
#         'Extremadamente problemático/ No puede hacerlo':5}

# # Ordenar el array por el orden definido en el diccionario
# data_sorted = sorted(data, key=lambda x: priority_order[x[1]])

# # Mostrar el resultado
# for row in data_sorted:
#     print(row)

data = [
    [2022, 'Extranjero', 65259],
    [2023, 'Extranjero', 59949],
    [2015, 'Chile', 787455],
    [2016, 'Chile', 809152],
    [2017, 'Chile', 814363]
]

# Iterar sobre el array y cambiar 'Chile' por 'Chileno' donde sea que se encuentre
for subarray in data:
    for i, value in enumerate(subarray):
        if value == 'Chile':
            subarray[i] = 'Chileno'

# Ver el resultado
print(data)
