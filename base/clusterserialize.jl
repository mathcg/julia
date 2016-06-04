# This file is a part of Julia. License is MIT: http://julialang.org/license
import .Serialization: known_object_data, object_number, serialize_cycle, deserialize_cycle, writetag,
                      __deserialized_types__, serialize_typename_body, deserialize_typename_body,
                      TYPENAME_TAG

type ClusterSerializer{I<:IO} <: AbstractSerializer
    io::I
    counter::Int
    table::ObjectIdDict

    sent_objects::Dict{UInt64, Bool} # used by serialize (track objects sent)

    ClusterSerializer(io::I) = new(io, 0, ObjectIdDict(), Dict())
end
ClusterSerializer(io::IO) = ClusterSerializer{typeof(io)}(io)

function deserialize(s::ClusterSerializer, ::Type{TypeName})
    number, full_body_sent = deserialize(s)
    makenew = false
    if !full_body_sent
        if !haskey(known_object_data, number)
            error("Expected object in cache. Not found.")
        else
            tn = known_object_data[number]::TypeName
        end
    else
        name = deserialize(s)
        mod = deserialize(s)
        if haskey(known_object_data, number)
            # println(mod, ":", name, ", id:", number, " should NOT have been sent")
            warn("Object in cache. Should not have been resent.")
        elseif isdefined(mod, name)
            tn = getfield(mod, name).name
            # TODO: confirm somehow that the types match
            warn(mod, ":",name, " isdefined, need not have been serialized")
            name = tn.name
            mod = tn.module
        else
            name = gensym()
            mod = __deserialized_types__
            tn = ccall(:jl_new_typename_in, Ref{TypeName}, (Any, Any), name, mod)
            makenew = true
        end
    end
    deserialize_cycle(s, tn)
    full_body_sent && deserialize_typename_body(s, tn, number, name, mod, makenew)
    makenew && (known_object_data[number] = tn)
    return tn
end

function serialize(s::ClusterSerializer, t::TypeName)
    serialize_cycle(s, t) && return
    writetag(s.io, TYPENAME_TAG)

    identifier = object_number(t)
    if t.module === Main && !haskey(s.sent_objects, identifier)
        serialize(s, (identifier, true))
        serialize(s, t.name)
        serialize(s, t.module)
        serialize_typename_body(s, t)
        s.sent_objects[identifier] = true
#        println(t.module, ":", t.name, ", id:", identifier, " sent")
    else
        serialize(s, (identifier, false))
#        println(t.module, ":", t.name, ", id:", identifier, " NOT sent")
    end
end

# TODO
# Handle wrkr1 -> wrkr2, wrkr1 -> wrkr3 and wrkr2 -> wrkr3
# Handle other modules
