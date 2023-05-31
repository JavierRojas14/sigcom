"""
Programa para obtener el formato 4 de Suministros del SIGCOM. Unidad de Finanzas.
Javier Rojas Benítez"""

import os
import json

import numpy as np
import pandas as pd

import itertools

with open("maestro_articulos_sigcom.json", encoding="utf-8") as file:
    MAESTRO_ARTICULOS = json.load(file)

from constantes import (
    DESTINO_INT_CC_SIGCOM,
    DICCIONARIO_UNIDADES_A_DESGLOSAR,
    WINSIG_SERVICIO_FARMACIA_CC_SIGCOM,
    DICCIONARIO_PRODUCIONES_SIGCOM
)

DICCIONARIO_UNIDADES_A_DESGLOSAR = dict(
    itertools.islice(DICCIONARIO_UNIDADES_A_DESGLOSAR.items(), 7)
)

pd.options.mode.chained_assignment = None  # default='warn'


class AnalizadorSuministros:
    """
    Esta clase permite desglosar los suministros de la cartola valorizada del SCI, y generar
    el formato 4 de Suministros del SIGCOM.
    """

    def __init__(self):
        pass

    def correr_programa(self):
        """
        Esta es la función principal para correr el programa. Ejecuta las siguientes funciones:

        1 - Leer, Traducir y filtrar la cartola valorizada del SCI.
        2 - Permite rellenar los artículos que NO tengan un destino asociado en el INT.
        3 - Rellena el formato del SIGCOM.
        4 - Guarda los archivos generados
        """
        df_cartola = self.leer_asociar_y_filtrar_cartola()
        df_completa = self.rellenar_destinos(df_cartola)
        formato_relleno = self.convertir_a_tabla_din_y_rellenar_formato(df_completa)

        formato_desglosado = self.desglosar_por_produccion(formato_relleno)

        self.guardar_archivos(
            formato_relleno=formato_relleno,
            formato_desglosado=formato_desglosado,
            df_completa=df_completa,
        )

    def leer_asociar_y_filtrar_cartola(self):
        """
        Esta función controla el flujo de creación de la cartola traducida.
        Si NO existe la cartola traducida, entonces crea una nueva desde la cartola cruda.
        Si existe una cartola traducida, entonces lee esta y la trata.
        """
        if "cartola_valorizada_traducida.xlsx" not in os.listdir("input"):
            df_filtrada = self.leer_cartola_desde_cero()
            df_filtrada.to_excel("input\\cartola_valorizada_traducida.xlsx", index=False)

        else:
            df_filtrada = pd.read_excel("input\\cartola_valorizada_traducida.xlsx")

        return df_filtrada

    def leer_cartola_desde_cero(self):
        """
        Esta función permite leer el archivo de la Cartola Valorizada del SCI. Luego, trata
        este archivo de la siguiente forma:

        1 - Crea una copia de la cartola.
        2 - Deja solamente los movimientos de salida de los artículos
        3 - Filtra todos los movimientos que NO tengan FARMACIA en su nombre, exceptuando
        SECRE. FARMACIA
        4 - Filtra todos los movimientos que tengan como motivo a "Merma" - "Préstamo" o "Devolución
        al Proveedor".
        5 - Asocia el código de bodega con el código SIGFE y el código SIGCOM.
        6 - Asocia el destino INT (destino del artículo dentro del hospital) con un centro
        de costo.
        7 - Filtra todos los artículos que sean del tipo Farmacia (ya que estos vienen desde
        la planilla de Juan Pablo).
        """
        df_cartola = pd.read_csv("input\\Cartola valorizada.csv")
        df_filtrada = df_cartola.copy()

        df_filtrada = df_filtrada.query('Movimiento == "Salida"')
        mask_farmacia = ~(df_filtrada["Destino"].str.contains("FARMACIA")) | (
            df_filtrada["Destino"].str.contains("SECRE. FARMACIA")
        )
        df_filtrada = df_filtrada[mask_farmacia]
        motivos_a_filtrar = ["Merma", "Préstamo", "Devolución al Proveedor"]
        df_filtrada = df_filtrada[~df_filtrada["Motivo"].isin(motivos_a_filtrar)]

        df_filtrada = self.asociar_codigo_articulo_a_sigcom(df_filtrada)
        df_filtrada = self.asociar_destino_int_a_sigcom(df_filtrada)
        df_filtrada = df_filtrada.query('Tipo_Articulo_SIGFE != "Farmacia"')
        df_filtrada = df_filtrada.sort_values(["CC SIGCOM", "Nombre"], na_position="first")

        return df_filtrada

    def asociar_codigo_articulo_a_sigcom(self, df_cartola):
        """
        Esta función permite relacionar el código de bodega con el código presupuestario
        SIGCOM y SIGFE.
        """
        df_filtrada = df_cartola.copy()
        df_filtrada["Tipo_Articulo_SIGCOM"] = df_filtrada["Codigo Articulo"].apply(
            lambda x: MAESTRO_ARTICULOS[x]["Total_SIGCOM"]
        )

        df_filtrada["Tipo_Articulo_SIGFE"] = df_filtrada["Codigo Articulo"].apply(
            lambda x: MAESTRO_ARTICULOS[x]["Item SIGFE"]
        )

        return df_filtrada

    def asociar_destino_int_a_sigcom(self, df_cartola):
        """
        Esta función permite asociar el destino INT con el centro de costo SIGCOM.
        """
        df_filtrada = df_cartola.copy()
        df_filtrada["CC SIGCOM"] = df_filtrada["Destino"].apply(lambda x: DESTINO_INT_CC_SIGCOM[x])

        return df_filtrada

    def rellenar_destinos(self, df_cartola):
        """
        Esta función permite rellenar todos los ítems que tengan algún destino que NO
        tenga relacionado algún centro de costo SIGCOM (Ej: Hospital del Salvador, INT, otros).
        """
        sin_cc = df_cartola[df_cartola["CC SIGCOM"].isna()]
        a_printear = sin_cc[["Nombre", "Destino", "Tipo_Articulo_SIGFE", "Tipo_Articulo_SIGCOM"]]

        print("\n- Se rellenarán los centros de costo NO ASIGNADOS asociados a cada artículo - \n")
        print(f"{a_printear.to_markdown()}")

        for nombre_articulo in sin_cc["Nombre"].unique():
            while True:
                destino = input(
                    f"\n{nombre_articulo}\n" f"Qué destino crees que es? (están en constantes.py): "
                )

                if destino in DESTINO_INT_CC_SIGCOM:
                    cc_sigcom = DESTINO_INT_CC_SIGCOM[destino]

                    mask_articulos_mismo_nombre = sin_cc["Nombre"] == nombre_articulo
                    a_cambiar = sin_cc[mask_articulos_mismo_nombre]
                    df_cartola.loc[a_cambiar.index, "Destino"] = destino
                    df_cartola.loc[a_cambiar.index, "CC SIGCOM"] = cc_sigcom
                    break

                else:
                    print("Debes ingresar un destino válido.")

            df_cartola.to_excel("input\\cartola_valorizada_traducida.xlsx", index=False)

        return df_cartola

    def convertir_a_tabla_din_y_rellenar_formato(self, df_consolidada):
        """
        Esta función permite convertir la cartola valorizada en una tabla al estilo wide.
        """
        tabla_dinamica = pd.pivot_table(
            df_consolidada,
            values="Neto Total",
            index="CC SIGCOM",
            columns="Tipo_Articulo_SIGCOM",
            aggfunc=np.sum,
        )

        formato = pd.read_excel("input\\Formato 4_Distribución Suministro 2022-12.xlsx")
        formato = formato.set_index("Centro de Costo")

        for centro_costo in tabla_dinamica.index:
            for item_sigcom in tabla_dinamica.columns:
                formato.loc[centro_costo, item_sigcom] = tabla_dinamica.loc[
                    centro_costo, item_sigcom
                ]

        return formato

    def desglosar_centro_de_costo(self, desglose, total_dinero):
        con_dinero = desglose.copy()
        con_dinero["TOTAL_X_PORCENTAJE"] = con_dinero["PORCENTAJES"] * total_dinero

        return con_dinero

    def desglosar_por_produccion(self, formato_relleno):
        """
        Esta función permite hacer el desglose, con los montos respectivos, de cada uno de los
        Centros de Costos que lo requieran. Solamente desglosa los que están en el formato.
        """
        producciones = pd.ExcelFile("input\\output_producciones.xlsx")

        for cc_a_desglosar, subunidades_a_asignar_dinero in DICCIONARIO_UNIDADES_A_DESGLOSAR.items():
            nombre_cortado = cc_a_desglosar[:31]
            print(f"Se va a desglosar {cc_a_desglosar} en {subunidades_a_asignar_dinero}")
            produccion_cc = pd.read_excel(producciones, sheet_name=nombre_cortado).iloc[:-1]
            produccion_cc['SIGCOM'] = produccion_cc['SERVICIOS FINALES'].apply(lambda x: DICCIONARIO_PRODUCIONES_SIGCOM[x])
            resumen_porcentajes = produccion_cc.groupby('SIGCOM')["PORCENTAJES"].sum()

            total = formato_relleno.loc[cc_a_desglosar, :].copy()
            for cc_subunidad, porcentaje_subunidad in resumen_porcentajes.items():
                print(f"Se esta asignando dinero a {cc_subunidad}, y tiene un porcentaje de {porcentaje_subunidad}")
                desglose = total * porcentaje_subunidad

                if cc_subunidad != cc_a_desglosar:
                    dinero_previo = formato_relleno.loc[cc_subunidad]
                    desglose = desglose.add(dinero_previo, fill_value=0)

                formato_relleno.loc[cc_subunidad] = desglose
            
            print()

        return formato_relleno

    def guardar_archivos(self, **kwargs):
        """
        Esta función permite guardar los archivos generados en el programa.
        """
        with pd.ExcelWriter("output_suministros.xlsx") as writer:
            for nombre_hoja, df in kwargs.items():
                df.to_excel(writer, sheet_name=nombre_hoja)


analizador = AnalizadorSuministros()
analizador.correr_programa()
