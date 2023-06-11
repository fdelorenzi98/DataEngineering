CREATE TABLE IF NOT EXISTS franciscodlorenzi_coderhouse.tipo_cambio
(
  Fecha date DISTKEY,
  Cambio double precision
)
SORTKEY (Fecha);
