CREATE TABLE franciscodlorenzi_coderhouse.tipo_cambio
(
  Fecha date DISTKEY,
  Cambio double precision
)
SORTKEY (Fecha);
