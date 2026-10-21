-- The retention worker deletes raw messages by age. Rows arrive in received_at
-- order, so a BRIN index finds the old ones at a fraction of a B-tree's size and
-- write cost on the busiest table.
CREATE INDEX raw_ais_messages_received_at_brin ON raw_ais_messages USING brin (received_at);
