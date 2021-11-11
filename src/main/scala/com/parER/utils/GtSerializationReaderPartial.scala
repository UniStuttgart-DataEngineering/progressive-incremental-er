package com.parER.utils

import java.util
import java.util.stream.Collectors
import java.util.{List, Set}

import scala.compat.java8.FunctionConverters._
import org.scify.jedai.datamodel.{EntityProfile, IdDuplicates}
import org.scify.jedai.datareader.AbstractReader
import org.scify.jedai.datareader.groundtruthreader.GtSerializationReader

class GtSerializationReaderPartial(filePath:String, limitD1: Int, limitD2: Int) extends GtSerializationReader(filePath){
  override def getDuplicatePairs(profilesD1: util.List[EntityProfile], profilesD2: util.List[EntityProfile]): util.Set[IdDuplicates] = {
    if (!idDuplicates.isEmpty) return idDuplicates

    idDuplicates.addAll(
      AbstractReader.loadSerializedObject(inputFilePath).asInstanceOf[util.Set[IdDuplicates]].stream().filter(
        asJavaPredicate(d => d.getEntityId1 <= limitD1 && d.getEntityId2 <= limitD2)
      ).collect(Collectors.toSet[IdDuplicates])
    )

    //idDuplicates.addAll(AbstractReader.loadSerializedObject(inputFilePath).asInstanceOf[util.Set[IdDuplicates]])
    idDuplicates
  }
}
