
from decimal import Decimal, InvalidOperation


CAMPOS_OBLIGATORIOS = [
    "anho",
    "id_Mes",
    "empresaGenera",
    "planta",
]

CAMPOS_ENERGIA = [
    "energiaPunta",
    "energiaValle",
    "energiaNoche",
]


def validar_registro(registro, anho_esperado, numero_fila):
    """
    Valida un registro descargado desde ARESEP.

    Devuelve una lista de errores.
    Si la lista está vacía, el registro es válido.
    """

    errores = []

    # 1. Comprobar que recibimos un diccionario.
    if not isinstance(registro, dict):
        return [
            f"{anho_esperado}, fila {numero_fila}: estructura invalida"
        ]

    # 2. Comprobar campos obligatorios.
    for campo in CAMPOS_OBLIGATORIOS:
        valor = registro.get(campo)

        if valor is None or str(valor).strip() == "":
            errores.append(
                f"{anho_esperado}, fila {numero_fila}: falta {campo}"
            )

    # 3. Comprobar el mes y el año.
    try:
        mes = int(registro["id_Mes"])
        anho = int(registro["anho"])

        if not 1 <= mes <= 12:
            raise ValueError()

        if anho != anho_esperado:
            raise ValueError()

    except (ValueError, TypeError, KeyError):
        errores.append(
            f"{anho_esperado}, fila {numero_fila}: fecha invalida"
        )

    # 4. Comprobar los valores de energía.
    for campo in CAMPOS_ENERGIA:
        valor = registro.get(campo)

        if valor is None or valor == "":
            continue

        try:
            energia = Decimal(str(valor))

            if not energia.is_finite():
                raise InvalidOperation()

        except (InvalidOperation, ValueError, TypeError):
            errores.append(
                f"{anho_esperado}: {campo} no es numerico"
            )

    return errores
