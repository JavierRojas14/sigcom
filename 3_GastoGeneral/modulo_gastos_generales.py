"""
Este programa permite obtener el formato 3 de Gastos Generales del SIGCOM. Unidad de Finanzas.
Javier Rojas Benítez."""

import json
import os
from time import sleep

import numpy as np
import pandas as pd
import requests

from constantes import (
    CODIGOS_CENTRO_DE_COSTO,
    EXCEPCIONES_SIGFE,
    TICKET_MERCADO_PUBLICO,
    GASTOS_METROS_CUADRADOS,
    CC_M2_COSTOS,
)

pd.set_option("display.max_colwidth", None)
pd.options.mode.chained_assignment = None  # default='warn'


class ModuloGastosGeneralesSIGCOM:
    """Esta es la definición de la clase ModuloGastosGeneralesSIGCOM, que permite:

    1) Leer los archivos de estado de ejecución presupuestaria, estado de devengo,
    planilla PERC y el formato SIGOM para los gastos generales.
    """

    def __init__(self):
        pass

    def correr_programa(self):
        """
        Esta es la función principal del programa, permite correrlo de forma general.
        """
        estado_ej_presup, disponibilidad_devengo = self.cargar_archivos_y_tratar_df()

        (
            estado_ej_presup,
            facturas_a_gg,
            facturas_a_rrhh,
            facturas_a_fondos_fijos,
        ) = self.desglosar_gastos_generales_rrhh_fondo_fijo(
            estado_ej_presup, disponibilidad_devengo
        )

        facturas_a_gg = self.obtener_detalle_facturas(facturas_a_gg)
        facturas_a_gg = self.rellenar_centros_de_costos(facturas_a_gg)
        formato_rellenado_gg = self.rellenar_formato_gg(estado_ej_presup, facturas_a_gg)
        formato_rrhh = self.obtener_formato_rrhh(facturas_a_rrhh)

        self.guardar_archivos(
            formato_rellenado_gg,
            formato_rrhh,
            estado_ej_presup,
            facturas_a_gg,
            facturas_a_rrhh,
            facturas_a_fondos_fijos,
        )

    def cargar_archivos_y_tratar_df(self):
        """
        Esta función permite cargar los archivos necesarios para calcular los Gastos Generales
        (EjecuciónPresupuestaria y DisponbilidadDevengo)

        - El archivo ejecución presupuestaria lo carga, filtra por las columnas útiles ("Concepto
        Presupuestario" y "Devengado"), filtra los headers de las subtablas presentes y finalmente
        asigna los dtypes correctos (object, int64 y object; Concepto Presupuestario, Devengado
        y COD SIGFE).

        - Agrega la columna "COD SIGFE" a los dos primeros archivos. Además, los COD SIGFE
        están en formato str.
        """
        print("- Cargando los archivos -")
        estado_ej_presup = pd.read_excel("input\\SA_EstadoEjecucionPresupuestaria.xls", header=6)
        estado_ej_presup = estado_ej_presup[["Concepto Presupuestario", "Devengado"]]
        estado_ej_presup = estado_ej_presup.query('`Devengado` != "Devengado"')
        estado_ej_presup["Devengado"] = estado_ej_presup["Devengado"].astype(np.int64)
        estado_ej_presup["Devengado_merge"] = estado_ej_presup["Devengado"].astype(np.int64)
        estado_ej_presup["COD SIGFE"] = (
            estado_ej_presup["Concepto Presupuestario"].str.split().str[0]
        )

        ruta_archivo_dispo_devengo = "input\\SA_DisponibilidadDevengoPresupuestario.xls"
        disponibilidad_devengo = pd.read_excel(ruta_archivo_dispo_devengo, header=5)
        disponibilidad_devengo = disponibilidad_devengo[
            ["Titulo", "Principal", "Número Documento", "Concepto Presupuestario", "Monto Vigente"]
        ]
        disponibilidad_devengo["COD SIGFE"] = (
            disponibilidad_devengo["Concepto Presupuestario"].str.split().str[0]
        )
        disponibilidad_devengo["oc"] = disponibilidad_devengo["Titulo"].str.split("/").str[3]

        traductor_sigfe_sigcom = pd.read_excel("input\\relacion_sigfe_sigcom_cristian_GG.xlsx")
        traductor_sigfe_sigcom["COD SIGFE"] = traductor_sigfe_sigcom["COD SIGFE"].str.replace(
            "'", "", regex=False
        )
        traductor_sigfe_sigcom["COD SIGCOM"] = traductor_sigfe_sigcom["COD SIGCOM"].str.replace(
            "'", "", regex=False
        )

        estado_ej_presup = pd.merge(
            estado_ej_presup, traductor_sigfe_sigcom, how="inner", on="COD SIGFE"
        )
        disponibilidad_devengo = pd.merge(
            disponibilidad_devengo, traductor_sigfe_sigcom, how="inner", on="COD SIGFE"
        )

        return estado_ej_presup, disponibilidad_devengo

    def desglosar_gastos_generales_rrhh_fondo_fijo(self, estado_ej_presup, disponibilidad_devengo):
        """
        Esta función permite desglosar el detalle de cada ítem SIGFE, y sus facturas asociadas.

        - En este caso, se filtran las facturas asociadas a gastos por m2 (COD SIGCOM
        92, 93, 100, 133 y 170), ya que no es necesario analizarlas (Sin embargo, se podrían
        dejar para ver si los gastos coinciden con los de la ejecución presupuestaria

        - Luego, se guardan las facturas que van asociadas a gastos de RRHH, analizando las ex
        cepciones.

        - Después, se guardan las facturas que van asociadas a FONDOS FIJOS, y se guardan.

        - Finalmente, se sacan las facturas de los últimos dos apartados, y se obtienen las
        facturas asociadas a GG.
        """

        print("- Analizando la disponbilidad de devengo y sus facturas - \n")
        filtro_metros_cuadrados = disponibilidad_devengo["COD SIGCOM"].isin(GASTOS_METROS_CUADRADOS)

        facturas_a_gg = disponibilidad_devengo[~filtro_metros_cuadrados]

        facturas_a_rrhh, estado_ej_presup = self.obtener_excepciones_a_rrhh(
            facturas_a_gg, estado_ej_presup
        )
        facturas_a_gg = facturas_a_gg.drop(facturas_a_rrhh.index)

        facturas_a_fondos_fijos, estado_ej_presup = self.obtener_fondos_fijos(
            facturas_a_gg, estado_ej_presup
        )

        facturas_a_gg = facturas_a_gg.drop(facturas_a_fondos_fijos.index)

        return estado_ej_presup, facturas_a_gg, facturas_a_rrhh, facturas_a_fondos_fijos

    def obtener_excepciones_a_rrhh(self, facturas_a_analizar, estado_ej_presup):
        """
        Con esta función se obtienen las excepciones de Gastos generales que van a RRHH.
        Como por ejemplo: FERNANDO BARAONA/CARDIOCIRUGIA, otros.
        """
        facturas_a_rrhh = pd.DataFrame()

        for codigo_sigfe_excepcion in EXCEPCIONES_SIGFE:
            print(f"\n Analizando la excepcion: {codigo_sigfe_excepcion} \n")
            query_excepcion = facturas_a_analizar.query("`COD SIGFE` == @codigo_sigfe_excepcion")

            if codigo_sigfe_excepcion == "221299901601":
                mask_a_rrhh = query_excepcion["Principal"].str.contains("BARAONA")

            elif codigo_sigfe_excepcion == "221299901602":
                mask_a_rrhh = query_excepcion["Principal"].notna()

            elif codigo_sigfe_excepcion == "221299900902":
                mask_a_rrhh = query_excepcion["Principal"].str.contains("CARDIOLOGIA") | (
                    query_excepcion["Principal"].str.contains("CARDIOCIRUGIA")
                )

            elif codigo_sigfe_excepcion == "221299900201":
                mask_a_rrhh = query_excepcion["Principal"].str.contains("MANUEL MENESES")

            elif codigo_sigfe_excepcion == "221299900202":
                mask_a_rrhh = query_excepcion["Principal"].str.contains(
                    "ANDUEZA"
                ) | query_excepcion["Principal"].str.contains("CARDIOCIRUGIA")

            df_a_rrhh = query_excepcion[mask_a_rrhh]
            df_a_gg = query_excepcion[~mask_a_rrhh]

            valor_a_rrhh = df_a_rrhh["Monto Vigente"].sum()
            valor_a_gg = df_a_gg["Monto Vigente"].sum()

            print(
                f"Las siguientes facturas irán a RRHH: \n"
                f'{df_a_rrhh[["Titulo", "Monto Vigente"]].to_markdown()} \n'
                f"El monto destinado a RRHH será de: {valor_a_rrhh} \n"
            )

            print(
                f"Las siguientes facturas irán a GG: \n"
                f'{df_a_gg[["Titulo", "Monto Vigente"]].to_markdown()} \n'
                f"El monto destinado a GG será de: {valor_a_gg} \n"
            )

            facturas_a_rrhh = pd.concat([facturas_a_rrhh, df_a_rrhh])
            mask_excepcion = estado_ej_presup["COD SIGFE"] == codigo_sigfe_excepcion

            estado_ej_presup.loc[mask_excepcion, "Devengado_merge"] = valor_a_gg
            estado_ej_presup.loc[mask_excepcion, "Costo_a_gg"] = valor_a_gg
            estado_ej_presup.loc[mask_excepcion, "Costo_a_rrhh"] = valor_a_rrhh

        return facturas_a_rrhh, estado_ej_presup

    def obtener_fondos_fijos(self, facturas_a_analizar, estado_ej_presup):
        """
        Con esta función se sacan los fondos fijos que hay dentro de las facturas a analizar
        """
        print("\n - Analizando fondos fijos -\n")
        mask_fondos_fijos = facturas_a_analizar["Titulo"].str.upper().str.contains("FIJO")

        facturas_a_fondos_fijos = facturas_a_analizar[mask_fondos_fijos]

        columnas_para_print = ["Titulo", "Principal", "COD SIGFE", "COD SIGCOM"]
        formato_print = facturas_a_fondos_fijos[columnas_para_print].to_markdown()
        print(f"Las facturas que van a fondos fijos son: \n {formato_print}")

        suma_fondos_fijos = facturas_a_fondos_fijos.groupby("COD SIGFE").sum()
        print(
            f"\nY suman lo siguiente (esto se va a descontar): \n"
            f"{suma_fondos_fijos.to_markdown()}"
        )

        for codigo_sigfe in suma_fondos_fijos.index:
            monto = suma_fondos_fijos.loc[codigo_sigfe, "Monto Vigente"]

            mask_fondo_fijo = estado_ej_presup["COD SIGFE"] == codigo_sigfe
            estado_ej_presup.loc[mask_fondo_fijo, "Descuento_fondo_fijo"] = monto
            resta_fondo = estado_ej_presup.loc[mask_fondo_fijo, "Devengado_merge"] - monto

            estado_ej_presup.loc[mask_fondo_fijo, "Devengado_merge"] = resta_fondo

        return facturas_a_fondos_fijos, estado_ej_presup

    def obtener_detalle_facturas(self, facturas_a_gg):
        """
        Esta función permite obtener el detalle de cada factura involucrada en el gasto general
        del item SIGCOM.

        Para esto, toma los items presupuestarios involucrados en el gasto general y busca las
        facturas en la disponibilidad de devengo.
        """
        print("- Se buscarán las ordenes de compra en marcado público -\n")

        if "facturas_gg_con_detalle_de_oc.xlsx" in os.listdir("input"):
            print("Ya existe un archivo con el detalle de las facturas, se leerá ese archivo.")
            facturas_a_gg = pd.read_excel("input\\facturas_gg_con_detalle_de_oc.xlsx")

        else:
            mask_con_oc = facturas_a_gg["oc"].str.contains("-")
            facturas_a_buscar = facturas_a_gg[mask_con_oc]

            cols_a_mostrar = ["Titulo", "Número Documento", "COD SIGCOM", "COD SIGFE"]
            print(facturas_a_buscar[cols_a_mostrar].to_markdown())

            facturas_a_gg["detalle_oc"] = facturas_a_buscar["oc"].apply(
                self.funcion_obtener_requests_mercado_publico
            )

            facturas_a_gg["centro_de_costo_asignado"] = None

            facturas_a_gg.to_excel("input\\facturas_gg_con_detalle_de_oc.xlsx", index=False)

        return facturas_a_gg

    def funcion_obtener_requests_mercado_publico(self, orden_de_compra):
        """
        Con esta función se obtie el detalle de las ordenes de compra que necesitan ser
        analizadas!."""
        print(f"Pidiendo la orden de compra: {orden_de_compra}")
        orden_de_compra = orden_de_compra.strip()

        url_request = (
            f"https://api.mercadopublico.cl/servicios/v1/publico/ordenesdecompra.json?"
            f"codigo={orden_de_compra}&"
            f"ticket={TICKET_MERCADO_PUBLICO}"
        )

        try:
            response = requests.get(url_request, timeout=None, verify=True)
            detalle_oc = json.dumps(
                response.json()["Listado"][0]["Items"], indent=1, ensure_ascii=False
            )

            sleep(2.0)

        except Exception as excepcion:
            print(type(excepcion), excepcion)
            detalle_oc = excepcion

        return detalle_oc

    def rellenar_centros_de_costos(self, facturas_a_gg):
        """
        Con esta función se va preguntando el centro de costo que va asignado a cada factura.
        En este caso, va printeando el detalle de cada factura.
        """
        print("\n- Se rellenarán los centros de costo NO ASIGNADOS asociados a cada factura - \n")
        mask_no_rellenadas = facturas_a_gg["centro_de_costo_asignado"].isna()
        facturas_no_rellenadas = facturas_a_gg[mask_no_rellenadas]

        for factura in facturas_no_rellenadas.itertuples():
            print("------------------------------------------------")
            print("------------------------------------------------")

            print(
                f"La factura {factura.Titulo} tiene el siguiente detalle: \n"
                f"CODIGO SIGFE: {factura._6} - {factura._8}\n"
                f"CODIGO SIGCOM: {factura._9} - {factura._10} \n\n"
                f"{factura.detalle_oc} \n"
            )

            while True:
                centro_de_costo = input(
                    "¿Qué centro de costo crees que es? "
                    "(Ingresar sólo el N° de código. "
                    "Los códigos están en constantes.py): "
                )

                if centro_de_costo in CODIGOS_CENTRO_DE_COSTO:
                    break

                else:
                    print("Debes ingresar un código válido.")

            facturas_a_gg.loc[factura.Index, "centro_de_costo_asignado"] = centro_de_costo
            print("------------------------------------------------")
            print("------------------------------------------------\n\n")

        facturas_a_gg.to_excel("input\\facturas_gg_con_detalle_de_oc.xlsx", index=False)

        return facturas_a_gg

    def rellenar_formato_gg(self, estado_ej_presup, facturas_a_gg):
        """
        Con esta función se toman todos los datos obtenidos previamente, y se rellena el
        formato 3 de Gastos Generales del SIGCOM.
        """
        print("- Rellenando la planilla formato -")
        formato_gg, indice_original, columnas_originales = self.obtener_formato_gastos_generales()
        estado_ej_presup_agrupado = estado_ej_presup.groupby("COD SIGCOM")["Devengado_merge"].sum()
        facturas_a_gg_agrupado = facturas_a_gg.groupby(
            by=["COD SIGCOM", "centro_de_costo_asignado"]
        )["Monto Vigente"].sum()

        for tipo_de_gasto, monto in estado_ej_presup_agrupado.items():
            formato_gg.loc["Valor General", tipo_de_gasto] = monto
            formato_gg.loc["Tipo de Distribución", tipo_de_gasto] = 4

        for indice, monto in facturas_a_gg_agrupado.items():
            tipo_de_gasto, centro_de_costo = indice
            tipo_de_gasto = str(tipo_de_gasto)
            centro_de_costo = str(centro_de_costo)
            formato_gg.loc[centro_de_costo, tipo_de_gasto] = monto

        for tipo_gasto_m2 in GASTOS_METROS_CUADRADOS:
            tipo_gasto_m2 = str(tipo_gasto_m2)
            formato_gg.loc["Tipo de Distribución", tipo_gasto_m2] = 1
            for centro_costo_m2, monto_m2 in CC_M2_COSTOS:
                centro_costo_m2 = str(centro_costo_m2)
                formato_gg.loc[centro_costo_m2, tipo_gasto_m2] = monto_m2

        # Ahora hay que sacar las columnas que tengan "Valor General" = 0.
        # Para esto hay que:
        # Iterar por las columnas, e identificar las que tengan un Valor General == 0
        columnas_sin_costo = formato_gg.loc["Valor General"] == 0
        formato_gg.loc[:, columnas_sin_costo] = None

        formato_gg.index = indice_original
        formato_gg = formato_gg.reset_index()
        formato_gg.columns = columnas_originales
        formato_gg = formato_gg.drop(
            columns=[
                "64-COMPRA DE INTERVENCIONES QUIRÚRGICAS INTRAHOSPITALARIAS CON PERSONAL EXTERNO",
                "65-COMPRA DE INTERVENCIONES QUIRÚRGICAS INTRAHOSPITALARIAS CON PERSONAL INTERNO",
            ]
        )

        return formato_gg

    def obtener_formato_gastos_generales(self):
        """
        Esta función carga el Formato 3 de Gastos Generales de SIGCOM. Además,
        lo formatea
        """
        formato_sigcom_gg = pd.read_excel("input\\Formato 3_Gasto General 2022-12.xlsx")
        indice_original = formato_sigcom_gg["Unnamed: 0"]
        columnas_originales = formato_sigcom_gg.columns

        formato_sigcom_gg["Unnamed: 0"] = formato_sigcom_gg["Unnamed: 0"].str.split("-").str[0]
        formato_sigcom_gg = formato_sigcom_gg.rename(
            columns={"Unnamed: 0": "centro_de_costo_asignado"}
        )
        formato_sigcom_gg = formato_sigcom_gg.set_index("centro_de_costo_asignado")

        formato_sigcom_gg.columns = formato_sigcom_gg.columns.str.split("-").str[0]

        return formato_sigcom_gg, indice_original, columnas_originales

    def obtener_formato_rrhh(self, facturas_a_rrhh):
        """
        Esta función obtiene las facturas asociadas a RRHH, y las formatea para que sean
        directamente imputadas en el Formato 2 de RRHH de SIGCOM."""

        print("\n- Obteniendo el formato para RRHH -")
        facturas_a_rrhh_agrupadas = (
            facturas_a_rrhh.groupby("Principal")["Monto Vigente"].sum().reset_index()
        )

        facturas_a_rrhh_agrupadas["Principal"] = facturas_a_rrhh_agrupadas["Principal"].str.split(
            n=1
        )

        facturas_a_rrhh_agrupadas["Rut"] = facturas_a_rrhh_agrupadas["Principal"].str[0]
        facturas_a_rrhh_agrupadas["Nombre"] = facturas_a_rrhh_agrupadas["Principal"].str[1]
        facturas_a_rrhh_agrupadas = facturas_a_rrhh_agrupadas[["Rut", "Nombre", "Monto Vigente"]]

        print(f"\n\n{facturas_a_rrhh_agrupadas.to_markdown()}")

        return facturas_a_rrhh_agrupadas

    def guardar_archivos(
        self,
        formato_rellenado_gg,
        formato_rrhh,
        estado_ej_presup,
        facturas_a_gg,
        facturas_a_rrhh,
        facturas_a_fondos_fijos,
    ):
        """
        Esta función toma todos los archivos generados durante el programa, y los guarda en un
        único archivo llamado output.xlsx"""
        with pd.ExcelWriter("output.xlsx") as writer:
            formato_rellenado_gg.to_excel(writer, sheet_name="formato_rellenado_gg", index=False)

            formato_rrhh.to_excel(writer, sheet_name="formato_rrhh", index=False)
            estado_ej_presup.to_excel(writer, sheet_name="estado_ej_presup", index=False)
            facturas_a_gg.to_excel(writer, sheet_name="facturas_a_gg", index=False)
            facturas_a_rrhh.to_excel(writer, sheet_name="facturas_a_rrhh", index=False)
            facturas_a_fondos_fijos.to_excel(
                writer, sheet_name="facturas_a_fondos_fijos", index=False
            )


objeto = ModuloGastosGeneralesSIGCOM()
objeto.correr_programa()
