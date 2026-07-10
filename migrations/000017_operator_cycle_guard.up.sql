-- Guard the operators ownership tree (parent_id, added in 000016) against
-- cycles, so the recursive CTEs in P4-3 can't spin. A BEFORE trigger walks up
-- from the proposed parent; if it reaches the row itself, the edge would close a
-- loop and is rejected. (The depth guard in the queries is a second belt.)

CREATE FUNCTION operators_reject_cycle() RETURNS trigger
LANGUAGE plpgsql AS $$
DECLARE
  cur INT;
BEGIN
  IF NEW.parent_id IS NULL THEN
    RETURN NEW;
  END IF;
  IF NEW.parent_id = NEW.id THEN
    RAISE EXCEPTION 'operator % cannot be its own parent', NEW.id;
  END IF;

  cur := NEW.parent_id;
  WHILE cur IS NOT NULL LOOP
    IF cur = NEW.id THEN
      RAISE EXCEPTION 'setting parent_id=% on operator % would create a cycle',
        NEW.parent_id, NEW.id;
    END IF;
    SELECT parent_id INTO cur FROM operators WHERE id = cur;
  END LOOP;

  RETURN NEW;
END;
$$;

CREATE TRIGGER operators_reject_cycle
  BEFORE INSERT OR UPDATE OF parent_id ON operators
  FOR EACH ROW EXECUTE FUNCTION operators_reject_cycle();
