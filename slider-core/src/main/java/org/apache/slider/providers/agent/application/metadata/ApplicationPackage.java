package org.apache.slider.providers.agent.application.metadata;

import java.util.ArrayList;
import java.util.List;

import org.apache.slider.core.exceptions.SliderException;

public class ApplicationPackage extends AbstractMetainfoSchema{
  private List<ComponentsInAddonPackage> components = new ArrayList<ComponentsInAddonPackage>();

  public void addComponent(ComponentsInAddonPackage component) {
    components.add(component);
  }

  //we must override getcomponent() as well. otherwise it is pointing to the overriden components of type List<Componet>
  public List<ComponentsInAddonPackage> getComponents(){
    return this.components;
  }

  @Override
  public String toString() {
    final StringBuilder sb =
        new StringBuilder("{");
    sb.append(",\n\"name\": ").append(name);
    sb.append(",\n\"comment\": ").append(comment);
    sb.append(",\n\"version\" :").append(version);
    sb.append(",\n\"components\" : {");
    for (ComponentsInAddonPackage component : components) {
      sb.append("\n").append(component);
    }
    sb.append("\n},");
    sb.append('}');
    return sb.toString();
  }

  @Override
  public void validate(String version) throws SliderException {
    if(name == null || name.isEmpty()){
      throw new SliderException("Missing name in metainfo.json for add on packages");
    }
    if(components.isEmpty()){
      throw new SliderException("Missing components in metainfo.json for add on packages");
    }
    for (ComponentsInAddonPackage component : components) {
      if(component.name == null || component.name.isEmpty()){
        throw new SliderException("Missing name of components in metainfo.json for add on packages");
      }
    }
  }

}
