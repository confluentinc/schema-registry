/*
 * Copyright 2020 Confluent Inc.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.confluent.kafka.schemaregistry.maven;

import org.codehaus.plexus.component.configurator.ComponentConfigurationException;
import org.codehaus.plexus.component.configurator.ConfigurationListener;
import org.codehaus.plexus.component.configurator.converters.ConfigurationConverter;
import org.codehaus.plexus.component.configurator.converters.composite.MapConverter;
import org.codehaus.plexus.component.configurator.converters.lookup.ConverterLookup;
import org.codehaus.plexus.component.configurator.expression.ExpressionEvaluator;
import org.codehaus.plexus.configuration.PlexusConfiguration;
import org.eclipse.sisu.inject.Logs;

import java.lang.reflect.Modifier;
import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.util.Map;
import java.util.TreeMap;

// Uses the fix from
// https://stackoverflow.com/questions/38628399/using-map-of-maps-as-maven-plugin-parameters
public class FixedMapConverter extends MapConverter {

  @Override
  public Object fromConfiguration(
      final ConverterLookup lookup,
      final PlexusConfiguration configuration,
      final Class<?> type,
      final Type[] typeArguments,
      final Class<?> enclosingType,
      final ClassLoader loader,
      final ExpressionEvaluator evaluator,
      final ConfigurationListener listener
  ) throws ComponentConfigurationException {
    final Object value = fromExpression(configuration, evaluator, type);
    if (null != value) {
      return value;
    }
    try {
      final Map<Object, Object> map = instantiateMap(configuration, type, loader);
      final Class<?> elementType = findElementType(typeArguments);
      if (Object.class == elementType || String.class == elementType) {
        for (int i = 0, size = configuration.getChildCount(); i < size; i++) {
          final PlexusConfiguration element = configuration.getChild(i);
          map.put(element.getName(), fromExpression(element, evaluator));
        }
        return map;
      }
      // handle maps with complex element types...
      final ConfigurationConverter converter = lookup.lookupConverterForType(elementType);
      for (int i = 0, size = configuration.getChildCount(); i < size; i++) {
        Object elementValue;
        final PlexusConfiguration element = configuration.getChild(i);
        try {
          elementValue = converter.fromConfiguration(lookup, element, elementType, enclosingType, //
              loader, evaluator, listener
          );
        } catch (final ComponentConfigurationException e) {
          // TEMP: remove when http://jira.codehaus.org/browse/MSHADE-168
          // is fixed
          elementValue = fromExpression(element, evaluator);

          Logs.warn("Map in " + enclosingType
                  + " declares value type as: {} but saw: {} at runtime",
              elementType, null != elementValue ? elementValue.getClass() : null);
        }
        // ~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~~
        map.put(element.getName(), elementValue);
      }
      return map;
    } catch (final ComponentConfigurationException e) {
      if (null == e.getFailedConfiguration()) {
        e.setFailedConfiguration(configuration);
      }
      throw e;
    }
  }

  @SuppressWarnings("unchecked")
  private Map<Object, Object> instantiateMap(
      final PlexusConfiguration configuration, final Class<?> type, final ClassLoader loader
  ) throws ComponentConfigurationException {
    final Class<?> implType = getClassForImplementationHint(type, configuration, loader);
    if (null == implType || Modifier.isAbstract(implType.getModifiers())) {
      return new TreeMap<Object, Object>();
    }

    final Object impl = instantiateObject(implType);
    return (Map<Object, Object>) impl;
  }

  private static Class<?> findElementType(final Type[] typeArguments) {
    if (null != typeArguments && typeArguments.length > 1) {
      if (typeArguments[1] instanceof Class<?>) {
        return (Class<?>) typeArguments[1];
      }
      // begin fix here
      if (typeArguments[1] instanceof ParameterizedType) {
        return (Class<?>) ((ParameterizedType) typeArguments[1]).getRawType();
      }
      // end fix here
    }
    return Object.class;
  }

}
