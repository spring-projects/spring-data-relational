package org.springframework.data.r2dbc.core

import org.springframework.data.mapping.MappingException
import kotlin.reflect.KProperty
import kotlin.reflect.jvm.javaField

inline fun <reified T : Any, C : Any> R2dbcEntityTemplate.column(property: KProperty<C>): String = property.javaField?.let {
    this.getColumnName(T::class.java, it)
} ?: throw MappingException("property is not valid")
