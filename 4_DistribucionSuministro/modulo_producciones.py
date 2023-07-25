"""
Este es un archivo para hacer un resumen de las producciones por Unidad
"""
import os
import sys
import json

import pandas as pd

from constantes import (
    DICCIONARIO_UNIDADES_A_DESGLOSAR,
    PORCENTAJES_A_CONSULTAS_ONCOLOGIA,
    PORCENTAJES_A_PROCEDIMIENTOS_ONCOLOGIA,
    UNIDADES_PROPORCIONALES_A_LA_PRODUCCION,
    VALOR_TAVI_SUMINISTROS,
    VALOR_EBUS_SUMINISTROS,
    VALOR_ECMO_SUMINISTROS,
    PORCENTAJES_A_CONSULTAS_CARDIOLOGIA,
    PORCENTAJES_A_CONSULTAS_ONCOLOGIA,
    PORCENTAJES_A_PROCEDIMIENTOS_CARDIOLOGIA,
    PORCENTAJES_A_PROCEDIMIENTOS_ONCOLOGIA,
    VALOR_CONSULTAS_ADMIN_SUMINISTROS,
)

pd.options.mode.chained_assignment = None  # default='warn'


class ModuloProducciones:
    """
    Esta clase permite obtener el desglose/análisis de cada una de las producciones presentes
    en el archivo de Producciones del INT.
    """

    def __init__(self):
        pass

    def correr_programa(self):
        """
        Esta funcion permite correr el programa para analizar las producciones mensuales del INT
        """
        df_hosp, df_prod = self.cargar_archivo()
        producciones_por_unidad = self.obtener_desglose_por_unidad(df_prod)
        self.guardar_archivos(producciones_por_unidad, df_hosp)

    def cargar_archivo(self):
        """
        Esta función permite cargar el archivo de producciones, y obtener el mes que se
        quiera analizar.
        """
        mes_a_analizar = sys.argv[1]

        nombre_archivo = [nombre for nombre in os.listdir("input") if "Producción" in nombre][0]
        nombre_archivo = os.path.join("input", nombre_archivo)
        producciones = pd.read_excel(nombre_archivo)

        producciones = producciones.loc[:, "SERVICIOS FINALES":"TOTAL AÑO"]
        producciones.columns = [
            "SERVICIOS FINALES",
            "ENERO",
            "FEBRERO",
            "MARZO",
            "ABRIL",
            "MAYO",
            "JUNIO",
            "JULIO",
            "AGOSTO",
            "SEPTIEMBRE",
            "OCTUBRE",
            "NOVIEMBRE",
            "DICIEMBRE",
            "TOTAL AÑO",
        ]

        producciones["SERVICIOS FINALES"] = producciones["SERVICIOS FINALES"].fillna("PLACEHOLDER")

        df_hospitalizaciones = producciones.loc[0:1, ["SERVICIOS FINALES", mes_a_analizar]]
        df_producciones = producciones.loc[3:, ["SERVICIOS FINALES", mes_a_analizar]]

        return df_hospitalizaciones, df_producciones

    def obtener_desglose_por_unidad(self, df_produccion):
        """
        Esta función permite desglosar cada una de los centros de costo según se indicó
        en el DICCIONARIO_UNIDADES_A_DESGLOSAR en constantes.py. Si se quiere hacer un nuevo
        desglose, entonces se debe agregar ahí.

        Esta función retorna un diccionario del tipo {unidad_a_desglosar: DataFrame del desglose}
        """
        producciones_por_unidad = {}
        for unidad_a_desglosar, lista_subunidades in DICCIONARIO_UNIDADES_A_DESGLOSAR.items():
            for i, produccion_a_pedir in enumerate(lista_subunidades):
                mask_consulta = self.obtener_mask_de_unidad(df_produccion, produccion_a_pedir)
                if i == 0:
                    mask_total = mask_consulta

                else:
                    mask_total = mask_total | mask_consulta

            df_unidad = df_produccion[mask_total]
            df_unidad = df_unidad.groupby("SERVICIOS FINALES").sum().reset_index()
            print(df_unidad)
            df_unidad["PORCENTAJES"] = self.obtener_porcentajes(df_unidad, unidad_a_desglosar)

            suma_producciones = df_unidad.iloc[:, 1].sum()
            df_unidad.loc[len(df_unidad.index)] = [unidad_a_desglosar, suma_producciones, "1"]

            df_unidad["AGRUPACION"] = unidad_a_desglosar

            producciones_por_unidad[unidad_a_desglosar] = df_unidad

        return producciones_por_unidad

    def obtener_mask_de_unidad(self, df_prod, produccion_pedida):
        """
        Esta función contiene diversas masks para las producciones que se estén pidiendo.
        """
        diccionario_unidad = {
            "41107-TOMOGRAFÍA": df_prod["SERVICIOS FINALES"].str.contains("TOMOGRAFIA"),
            "41108-IMAGENOLOGÍA": df_prod["SERVICIOS FINALES"].str.contains("IMAGENOLOGIA"),
            "464-QUIRÓFANOS CARDIOVASCULAR": df_prod["SERVICIOS FINALES"]
            == "QUIROFANOS CARDIOVASCULAR",
            "484-QUIRÓFANOS TORACICA": df_prod["SERVICIOS FINALES"]
            == "QUIROFANOS CIRUGIA TORACICA",
            "51001-BANCO DE SANGRE": df_prod["SERVICIOS FINALES"] == "BANCO DE SANGRE",
            "518-LABORATORIO CLÍNICO": df_prod["SERVICIOS FINALES"] == "LABORATORIO CLINICO",
            "90-HOSPITALIZACIÓN QUIRÚRGICA": df_prod["SERVICIOS FINALES"].str.contains(
                "HOSPITALIZACION QUIRURGICA"
            ),
            "66-HOSPITALIZACIÓN MEDICINA INTERNA": df_prod["SERVICIOS FINALES"].str.contains(
                "HOSPITALIZACION MEDICINA INTERNA"
            ),
            "270-PROCEDIMIENTOS TAVI": df_prod["SERVICIOS FINALES"].str.contains("TAVI"),
            "264-PROCEDIMIENTOS EBUS": df_prod["SERVICIOS FINALES"] == "PROCEDIMIENTO EBUS",
            "15022-PROCEDIMIENTO DE NEUMOLOGÍA": df_prod["SERVICIOS FINALES"]
            == "PROCEDIMIENTO DE NEUMOLOGIA (apnea del sueño)",
            "253-PROCEDIMIENTOS DE HEMODINAMIA": df_prod["SERVICIOS FINALES"]
            == "PROCEDIMIENTOS DE HEMODINAMIA",
            "265-PROCEDIMIENTOS ECMO": df_prod["SERVICIOS FINALES"].str.contains(
                "PROCEDIMIENTO ECMO"
            ),
            "15105-CONSULTA CARDIOLOGÍA": df_prod["SERVICIOS FINALES"] == "CONSULTA CARDIOLOGIA",
            "15220-CONSULTA CIRUGIA CARDIACA": df_prod["SERVICIOS FINALES"]
            == "CONSULTA CIRUGIA CARDIACA",
            "15201-CONSULTA CIRUGÍA GENERAL": df_prod["SERVICIOS FINALES"]
            == "CONSULTA CIRUGIA GENERAL (cirugía torax)",
            "15026-PROCEDIMIENTOS DE CARDIOLOGÍA": df_prod["SERVICIOS FINALES"]
            == "PROCEDIMIENTO DE CARDIOLOGIA",
            "195-UNIDAD DE TRATAMIENTO INTENSIVO ADULTO": (
                df_prod["SERVICIOS FINALES"].str.contains("UNIDAD DE TRATAMIENTO INTENSIVO")
                & ~(df_prod["SERVICIOS FINALES"].str.contains("+", regex=False))
            ),
            "166-UNIDAD DE CUIDADOS INTENSIVOS": (
                df_prod["SERVICIOS FINALES"].str.contains("UNIDAD DE CUIDADOS INTENSIVOS")
                & ~(df_prod["SERVICIOS FINALES"].str.contains("+", regex=False))
            ),
            "15123-PROGRAMA MANEJO DEL DOLOR": df_prod["SERVICIOS FINALES"]
            == "CONSULTA MANEJO DEL DOLOR",
            "15107-CONSULTA ONCOLOGÍA": df_prod["SERVICIOS FINALES"] == "CONSULTA ONCOLOGIA",
            "15038-PROCEDIMIENTO ONCOLOGÍA": df_prod["SERVICIOS FINALES"]
            == "PROCEDIMIENTO ONCOLOGIA",
            "15008-CONSULTA NUTRICIÓN": df_prod["SERVICIOS FINALES"] == "CONSULTA NUTRICION",
            "15010-CONSULTA OTROS PROFESIONALES": df_prod["SERVICIOS FINALES"]
            == "CONSULTA OTROS PROFESIONALES",
            "15111-CONSULTA NEUMOLOGÍA": df_prod["SERVICIOS FINALES"]
            == "CONSULTA NEUMOLOGIA (broncopulmonar)",
        }

        mask = diccionario_unidad[produccion_pedida]
        return mask

    def obtener_porcentajes(self, produccion_unidad, unidad_a_desglosar):
        """
        Esta función permite obtener los porcentajes/valores totales por desglose de centro de
        costo SIGCOM"""
        if unidad_a_desglosar in UNIDADES_PROPORCIONALES_A_LA_PRODUCCION:
            return produccion_unidad.iloc[:, 1] / produccion_unidad.iloc[:, 1].sum()

        if unidad_a_desglosar == "253-PROCEDIMIENTOS DE HEMODINAMIA":
            # Aislar los procedimientos
            series_hemodinamia = produccion_unidad.copy()
            mask_procedimientos = (
                produccion_unidad["SERVICIOS FINALES"].str.contains("NEUMOLOGIA")
                | produccion_unidad["SERVICIOS FINALES"].str.contains("HEMODINAMIA")
                | produccion_unidad["SERVICIOS FINALES"].str.contains("ONCOLOGIA")
            )

            procedimientos_hemo = produccion_unidad[mask_procedimientos]

            porcentajes_hemo = procedimientos_hemo.iloc[:, 1] / procedimientos_hemo.iloc[:, 1].sum()

            series_hemodinamia.loc[porcentajes_hemo.index, "PORCENTAJES"] = porcentajes_hemo

            print(f"Hemodinamia se desglosó en:\n{series_hemodinamia.to_markdown()}\n")

            return series_hemodinamia["PORCENTAJES"]

        if unidad_a_desglosar == "15026-PROCEDIMIENTOS DE CARDIOLOGÍA":
            series_cardiologia = produccion_unidad.copy()

            mask_consultas_cardio = produccion_unidad["SERVICIOS FINALES"].str.contains("CONSULTA")
            consultas_cardio = produccion_unidad[mask_consultas_cardio]

            porcentajes_consultas_cardio = (
                consultas_cardio.iloc[:, 1] / consultas_cardio.iloc[:, 1].sum()
            ) * PORCENTAJES_A_CONSULTAS_CARDIOLOGIA

            procedimientos_cardio = produccion_unidad.query(
                "`SERVICIOS FINALES` == " '"PROCEDIMIENTO DE CARDIOLOGIA"'
            )
            porcentajes_proc_cardio = (
                procedimientos_cardio.iloc[:, 1] / procedimientos_cardio.iloc[:, 1].sum()
            ) * PORCENTAJES_A_PROCEDIMIENTOS_CARDIOLOGIA

            series_cardiologia.loc[
                porcentajes_consultas_cardio.index, "PORCENTAJES"
            ] = porcentajes_consultas_cardio

            series_cardiologia.loc[
                porcentajes_proc_cardio.index, "PORCENTAJES"
            ] = porcentajes_proc_cardio

            print(f"Cardiología se desglosó en:\n{series_cardiologia.to_markdown()}\n")

            return series_cardiologia["PORCENTAJES"]

        if unidad_a_desglosar == "TAVI_ECMO_EBUS":
            series_tavi_ecmo_ebus = produccion_unidad.copy()
            ecmo = produccion_unidad.query(
                '`SERVICIOS FINALES` == "PROCEDIMIENTO ECMO (1,5 horas c/u/)"'
            )
            valor_total_ecmo = ecmo.iloc[:, 1] * VALOR_ECMO_SUMINISTROS
            tavi = produccion_unidad.query(
                '`SERVICIOS FINALES` == "PROCEDIMIENTO TAVI (4 horas c/u)"'
            )
            ebus = produccion_unidad.query('`SERVICIOS FINALES` == "PROCEDIMIENTO EBUS"')
            valor_total_tavi = tavi.iloc[:, 1] * VALOR_TAVI_SUMINISTROS
            valor_total_ebus = ebus.iloc[:, 1] * VALOR_EBUS_SUMINISTROS

            series_tavi_ecmo_ebus.loc[valor_total_tavi.index, "PORCENTAJES"] = valor_total_tavi
            series_tavi_ecmo_ebus.loc[valor_total_ebus.index, "PORCENTAJES"] = valor_total_ebus
            series_tavi_ecmo_ebus.loc[valor_total_ecmo.index, "PORCENTAJES"] = valor_total_ecmo

            print(f"TAVI_ECMO_EBUS se imputaron con:\n{series_tavi_ecmo_ebus.to_markdown()}\n")

            return series_tavi_ecmo_ebus["PORCENTAJES"]

    def guardar_archivos(self, produccion_por_unidad, produccion_hospitalizaciones):
        """
        Esta función guarda el desglose de las producciones!
        """
        with pd.ExcelWriter("output_producciones.xlsx") as writer:
            for desglose_por_unidad, df_unidad in produccion_por_unidad.items():
                df_unidad.to_excel(writer, sheet_name=f"{desglose_por_unidad[:31]}", index=False)

            produccion_hospitalizaciones.to_excel(
                writer, sheet_name="PORCENTAJES_HOSP", index=False
            )


modulo_producciones = ModuloProducciones()
modulo_producciones.correr_programa()
