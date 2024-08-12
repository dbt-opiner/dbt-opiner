import sqlglot

r = sqlglot.parse_one("""
                      with t as (select b from table),
                      t2 as (select a from table_2)

                      select table.*, t2.a
                      from table
                      left join t2 on table.id = t2.id
                      """)
print(type(r))
