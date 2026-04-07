box.cfg({
    listen = 3301,
    memtx_memory = 1024 * 1024 * 1024 -- 1GB для 5M записей
})

box.schema.space.create('KV', {if_not_exists=true})
box.space.KV:format({
    {name='key', type='string'},
    {name='value', type='varbinary', is_nullable=true}
})
box.space.KV:create_index('primary', {
    type='tree',
    parts={'key'},
    unique=true,
    if_not_exists=true
})