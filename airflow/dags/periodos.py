
def seleccionar_anhos(modo: str, anho_actual: int) -> list[int]:
    """
    Determina los años que debe procesar el pipeline.

    mensual: año actual y anterior.
    historico: desde 2018 hasta el año actual.
    """

    if modo == "mensual":
        return [anho_actual - 1, anho_actual]

    if modo == "historico":
        return list(range(2018, anho_actual + 1))

    raise ValueError(
        f"Modo de carga no reconocido: {modo}"
    )
