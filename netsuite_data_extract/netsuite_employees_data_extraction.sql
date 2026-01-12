SELECT DISTINCT emp.EMPLOYEE_ID AS EMPLOYEE_ID,
                emp.full_name   AS EMPLOYEE_FULL_NAME,
                rmap.role_id    AS EMPLOYEE_ROLE_ID,
                rol.role_name   AS EMPLOYEE_ROLE_NAME,
                emp.status      AS EMPLOYEE_STATUS
FROM Employees emp
     JOIN Entity_role_map rmap  ON rmap.entity_id = emp.employee_id
     JOIN roles1 rol ON rmap.role_id = rol.role_id