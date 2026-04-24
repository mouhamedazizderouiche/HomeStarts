CREATE OR REPLACE FUNCTION public.get_columns(table_name text)
RETURNS TABLE(column_name text)
LANGUAGE sql
SECURITY DEFINER
AS $$
  SELECT c.column_name::text
  FROM information_schema.columns c
  WHERE c.table_schema = 'public'
    AND c.table_name = get_columns.table_name
  ORDER BY c.ordinal_position;
$$;

GRANT EXECUTE ON FUNCTION public.get_columns(text) TO anon, authenticated, service_role;
