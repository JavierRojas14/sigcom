"""
Este programa permite obtener el formato 1 de RRHH del SIGCOM. Unidad de Finanzas.
Javier Rojas Benítez."""

import os
import json

import pandas as pd

from constantes import (
    DICCIONARIO_CARGO_INT_CARGO_SIGCOM,
    CARGOS_RRHH_HONORARIOS,
    TRADUCTOR_CC_INT_CC_SIGCOM,
)


class ModuloRecursosHumanosSIGCOM:
    """
    Esta clase permite analizar los datos para obtener el Formato 1 de RRHH del SIGCOM.
    """

    def __init__(self):
        pass

    def correr_programa(self):
        """
        Esta función es el backbone del programa. Ejecuta las funciones más globales del programa.

        - En primer lugar, carga los archivos de los funcionarios contratados por leyes y por
        honorarios.
        - En segundo lugar, junta ambos archivos, haciendo un procesamiento de los campos
        redundantes, para así obtener una relación 1 RUT: 1 NOMBRE: 1 CARGO: 1 UNIDAD: 1 CONTRATO
        """
        df_leyes_juntas, honorarios = self.cargar_archivos_y_formatearlos()
        suma_leyes_honorarios = self.juntar_leyes_y_honorarios(df_leyes_juntas, honorarios)
        suma_leyes_honorarios_traducido = self.traducir_a_sigcom(suma_leyes_honorarios)
        formato_sigcom = self.agregar_columnas_formato(suma_leyes_honorarios_traducido)

        self.guardar_archivos(
            formato_sigcom=formato_sigcom,
            suma_leyes_honorarios_traducido=suma_leyes_honorarios_traducido,
            suma_leyes_honorarios=suma_leyes_honorarios,
            df_leyes_juntas=df_leyes_juntas,
            honorarios=honorarios,
        )

    def cargar_archivos_y_formatearlos(self):
        """
        Este función permite obtener los archivos de RRHH, tanto para los funcionarios contratados
        por Leyes, como los funcionarios que prestan servicios por Honorarios.

        En este caso, las 3 leyes del Hospital son juntadas en una única tabla.

        Las Columnas que se dejan tanto en Leyes, como Honorario, son:
         - ['RUT-DV', 'NOMBRE', 'UNIDAD', 'CARGO', 'TOTAL HABER']"""

        print(f'{"SE ESTÁN CARGANDO LOS ARCHIVOS":-^40}')
        leyes = self.cargar_leyes()
        honorarios = self.cargar_honorarios()
        leyes_procesada, honorarios_procesada = self.unificar_formatos(leyes, honorarios)

        return leyes_procesada, honorarios_procesada

    def cargar_leyes(self):
        """
        Esta función permite cargar los archivos de funcionarios contratados por Leyes.
        - Los archivos considerados como leyes, son todos los que tengan el string "TODOS" en su
        nombre.

        - En esta función, primero se cargan todas las leyes a un DataFrame. Luego, se filtran
        sus columnas por las columnas útiles (además, entre archivos tienen distintas cantidades
        de columnas). Finalmente, se concatenan entre sí a lo largo.
        """

        print(f'\n{"SE ESTÁN CARGANDO LAS LEYES":-^30}')
        nombres_leyes = [
            os.path.join("input", nombre) for nombre in os.listdir("input") if "TODOS" in nombre
        ]

        leyes_crudas = map(lambda x: pd.read_excel(x, header=2), nombres_leyes)

        columnas_contrato = [
            "RUT-DV",
            "NOMBRE",
            "UNIDAD",
            "CARGO",
            "LEY AFECTO",
            "TOTAL HABER",
            "HORAS / GRADOS",
        ]

        leyes_columnas_utiles = list(map(lambda x: x[columnas_contrato], leyes_crudas))
        leyes_juntas = pd.concat(leyes_columnas_utiles)
        leyes_juntas = leyes_juntas.query("`TOTAL HABER` > 0")

        return leyes_juntas

    def cargar_honorarios(self):
        """
        Esta función carga el archivo de funcionarios que trabajan por honorarios.

        - Se agrega la columna RUT-DV, ya que esta ausente en el archivo original
        - Se cambian los nombres de las columnas para que tengan el mismo nombre que las columnas
        de leyes.
        """

        print(f'\n{"SE ESTÁN CARGANDO LOS HONORARIOS":-^30}')
        nombre_archivo_honorarios = [nom for nom in os.listdir("input") if "PERC" in nom][0]
        nombre_archivo_honorarios = os.path.join("input", nombre_archivo_honorarios)
        honorarios = pd.read_excel(nombre_archivo_honorarios)

        honorarios["RUT-DV"] = honorarios["RUT"].astype(str) + "-" + honorarios["DV"].astype(str)
        columnas_honorario = [
            "RUT-DV",
            "NOMBRE",
            "UNIDAD O SERVICIO DONDE SE DESEMPEÑA",
            "CARGO",
            "VALOR TOTAL O BRUTO",
            "CANTIDAD DE HORAS ORDINARIAS",
        ]

        honorarios = honorarios[columnas_honorario].rename(
            columns={
                "UNIDAD O SERVICIO DONDE SE DESEMPEÑA": "UNIDAD",
                "VALOR TOTAL O BRUTO": "TOTAL HABER",
            }
        )
        honorarios["LEY AFECTO"] = " "
        honorarios = honorarios.query("`TOTAL HABER` > 0")

        return honorarios

    def unificar_formatos(self, *args):
        """
        Esta función es un pasador para aplicar la funcion self.formatear_df() a los argumentos
        de la función.
        """
        print(f'\n{"SE ESTÁN UNIFICANDO LOS FORMATOS":-^30}')
        lista_dfs_formateadas = list(map(self.formatear_df, args))
        return lista_dfs_formateadas

    def formatear_df(self, df_funcionarios):
        """
        Esta función unifica las columnas "NOMBRE" y "RUT-DV" de la DataFrame que le llegue.
        Además, pone a la columna RUT-DV como índice.
        """

        df_funcionarios["NOMBRE"] = df_funcionarios["NOMBRE"].str.strip().str.upper()
        df_funcionarios["RUT-DV"] = df_funcionarios["RUT-DV"].str.strip().str.upper()
        df_funcionarios["UNIDAD"] = df_funcionarios["UNIDAD"].str.strip().str.upper()
        df_funcionarios["CARGO"] = df_funcionarios["CARGO"].str.strip().str.upper()

        df_funcionarios = df_funcionarios.set_index("RUT-DV")

        return df_funcionarios

    ############################################################################################
    def juntar_leyes_y_honorarios(self, df_leyes_juntas, honorarios):
        """
        Esta función permite unir los funcionarios por leyes y los por honorario.

        1. En primer lugar, la tabla de leyes y la de honorario se deben consolidar por NOMBRE,
        CARGO y UNIDAD.

        2. Una vez esos campos consolidados, se agrupan para obtener la suma total de haberes
        para ese funcionario

        3. Luego, a cada suma se le agrega un identificador de TIPO_CONTRATA (1 ley - 2 honorario)

        4. Posteriormente, se concatenan los funcionarios consolidados de leyes y honorarios.

        5. Luego, nuevamente se consolida NOMBRE, CARGO, UNIDAD, y en este caso TIPO_CONTRATA
        también.

        """
        print(f'\n{"SE EMPEZARÁ A JUNTAR LAS LEYES CON LOS HONORARIOS":-^50}')
        informacion_a_consolidar = ["NOMBRE", "CARGO", "UNIDAD", "LEY AFECTO"]
        leyes_modificada = df_leyes_juntas.copy()
        honorarios_modificada = honorarios.copy()

        # Hasta aqui si estan los ruts
        print(f'\n{"UNIFICANDO LAS LEYES Y HONORARIOS POR SEPARADO":-^50}')
        for info in informacion_a_consolidar:
            print(f'\n{"UNIFICANDO TODAS LAS LEYES JUNTAS":-^50}')
            leyes_modificada = self.unificar_redundancias(leyes_modificada, info)
            print(f'\n{"UNIFICANDO HONORARIOS":-^50}')
            honorarios_modificada = self.unificar_redundancias(honorarios_modificada, info)

        suma_leyes = (
            leyes_modificada.groupby(["RUT-DV"] + informacion_a_consolidar).sum().reset_index()
        )

        suma_honorarios = (
            honorarios_modificada.groupby(["RUT-DV"] + informacion_a_consolidar).sum().reset_index()
        )

        suma_leyes["TIPO_CONTRATA"] = "1"
        suma_honorarios["TIPO_CONTRATA"] = "2"

        ley_honorario = pd.concat([suma_leyes, suma_honorarios])
        ley_honorario = ley_honorario.set_index("RUT-DV")

        ley_honorario_modificada = ley_honorario.copy()

        print(f'\n{"UNIFICANDO LAS LEYES Y HONORARIOS JUNTOS":-^50}')
        informacion_a_consolidar_juntos = [
            "NOMBRE",
            "CARGO",
            "UNIDAD",
            "LEY AFECTO",
            "TIPO_CONTRATA",
        ]

        for info in informacion_a_consolidar_juntos:
            ley_honorario_modificada = self.unificar_redundancias(ley_honorario_modificada, info)

        suma_ley_honorario = (
            ley_honorario_modificada.groupby(["RUT-DV"] + informacion_a_consolidar_juntos)
            .sum(numeric_only=True)
            .reset_index()
        )

        suma_ley_honorario["HORAS TOTALES"] = (
            suma_ley_honorario["HORAS / GRADOS"]
            + suma_ley_honorario["CANTIDAD DE HORAS ORDINARIAS"]
        )

        return suma_ley_honorario

    def unificar_redundancias(self, df_funcionarios, redundancia_a_identificar):
        """
        Esta función permite identificar, crear un cambiador y unificar una característica
        de un df.

        - Retorna el df con la característica unificada!
        """
        df_repetidos = self.identificar_redundancias(df_funcionarios, redundancia_a_identificar)
        caract_seleccionadas = self.seleccionar_caract_a_unificar(
            df_repetidos, redundancia_a_identificar
        )

        df_funcionarios_unificados = self.cambiar_redundancias(
            df_funcionarios, caract_seleccionadas, redundancia_a_identificar
        )

        string_caract_seleccionadas = json.dumps(caract_seleccionadas, indent=1)
        print(
            f"Se identificaron las siguientes redundancias de {redundancia_a_identificar}:\n{df_repetidos} \n"
            f"Se consolidaron de la siguiente forma:\n{string_caract_seleccionadas} \n"
        )

        return df_funcionarios_unificados

    def identificar_redundancias(self, df_funcionarios, caracteristica_a_identificar):
        """
        Esta función permite identificar las redundancias en la caracteristica señalada

        - Retorna un DataFrame con el formato RUT-DV; caracteristica_a_identificar; TOTAL HABER
        - Los grupos están ordenados de forma descendente (La caract que gana más al principio).
        """

        agrupado = df_funcionarios.groupby(by=["RUT-DV", caracteristica_a_identificar])[
            "TOTAL HABER"
        ].sum(numeric_only=True)

        duplicados = agrupado[agrupado.index.get_level_values(0).duplicated(keep=False)]
        duplicados_ordenados = duplicados.reset_index().sort_values(
            by=["RUT-DV", "TOTAL HABER"], ascending=False
        )

        return duplicados_ordenados

    def seleccionar_caract_a_unificar(self, df_con_duplicados, caracteristica):
        """
        - Esta función permite seleccionar las características que se dejarán finalmente para
        ese funcionario.
        - Esta función asume que el df proporcionado solamente tiene 2 registros por funcionario.
        - Selecciona la primera característica del funcionario (o sea, en donde gana más).
        - Si la característica a seleccionar es CONTRATO, entonces para ese funcionario siempre
        se dejará el tipo de contrato 1 (por ley).

        """
        a_dejar = df_con_duplicados.groupby("RUT-DV").head(1)
        if caracteristica != "TIPO_CONTRATA":
            diccionario = dict(zip(a_dejar["RUT-DV"], a_dejar[caracteristica]))

        else:
            diccionario = dict(map(lambda x: (x, "1"), a_dejar["RUT-DV"]))

        return diccionario

    def cambiar_redundancias(self, df_funcionarios, dict_a_cambiar, caracteristica_a_cambiar):
        """
        - Esta función toma el DataFrame original de los funcionarios y aplica la consolidación
        a cada funcionario presente en el diccionario.
        """

        df_funcionarios_unificados = df_funcionarios.copy()
        for rut, caracteristica in dict_a_cambiar.items():
            df_funcionarios_unificados.loc[rut, caracteristica_a_cambiar] = caracteristica

        return df_funcionarios_unificados

    def traducir_a_sigcom(self, df_sumada):
        """
        Esta función permite asociar la Unidad del INT, con el CC del SIGCOM
        """
        df_traducida = df_sumada.copy()
        df_traducida["CC SIGCOM"] = df_traducida["UNIDAD"].apply(
            lambda x: TRADUCTOR_CC_INT_CC_SIGCOM[x]
        )
        df_traducida["Tipo Cargo SIGCOM"] = (
            df_traducida["CARGO"].str.upper().apply(lambda x: CARGOS_RRHH_HONORARIOS[x])
        )
        df_traducida["Codigo Cargo SIGCOM"] = df_traducida["Tipo Cargo SIGCOM"].apply(
            lambda x: DICCIONARIO_CARGO_INT_CARGO_SIGCOM[x]
        )

        return df_traducida

    def agregar_columnas_formato(self, suma_leyes_honorarios_traducido):
        df_nueva = suma_leyes_honorarios_traducido.copy()
        df_nueva[["Bonificaciones", "Beneficios Laborales"]] = 0
        df_nueva["Niveles Laborales"] = "00"
        df_nueva = df_nueva[
            [
                "RUT-DV",
                "NOMBRE",
                "TOTAL HABER",
                "Codigo Cargo SIGCOM",
                "Niveles Laborales",
                "Bonificaciones",
                "Beneficios Laborales",
                "TIPO_CONTRATA",
            ]
        ]

        df_nueva.columns = [
            "Rut",
            "Nombre",
            "Salario Base",
            "Categoría de Empleado",
            "Niveles Laborales",
            "Bonificaciones",
            "Beneficios laborales",
            "Tipo de Contrato",
        ]

        return df_nueva

    def guardar_archivos(self, **kwargs):
        """
        Esta función permite guardar 3 archivos:

        - La suma TOTAL de HABERES para cada uno de los funcionarios, previamente unificando
        sus NOMBRES, CARGOS, UNIDADES y TIPO CONTRATA. Por lo tanto, se obtiene una relación
        1:1:1:1 para un único funcionario.

        - El archivo de leyes juntas, previamente a unificar NOMBRE, CARGO y UNIDAD

        - El archivo de honorarios juntas, previamente a unificar NOMBRE, CARGO y UNIDAD.
        """
        with pd.ExcelWriter("output.xlsx") as writer:
            for nombre_df, df in kwargs.items():
                df.to_excel(writer, sheet_name=nombre_df, index=False)


modulo_rrhh_sigcom = ModuloRecursosHumanosSIGCOM()
modulo_rrhh_sigcom.correr_programa()
