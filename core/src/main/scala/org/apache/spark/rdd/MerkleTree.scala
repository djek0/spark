/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.spark.rdd

import java.io._
import org.apache.spark.internal.Logging

class MerkleTree(val root: MerkleNode) extends Serializable with Logging {
  
  def this(data: Array[(Long, String)]) = {
    this(MerkleTree.buildTree(data))
  }
  
  def rootHash: Int = root.hash
  
  def height: Int = {
    def loop(node: MerkleNode): Int = node match {
      case LeafNode(_, _) => 1
      case InternalNode(_, left, right) =>
        math.max(loop(left), loop(right)) + 1
    }
    loop(root)
  }
  
  def leafCount: Int = {
    def loop(node: MerkleNode): Int = node match {
      case LeafNode(_, _) => 1
      case InternalNode(_, left, right) =>
        if (left eq right) loop(left) else loop(left) + loop(right)
    }
    loop(root)
  }
  
  def saveToFile(filepath: String): Unit = {
    val file = new File(filepath)
    file.getParentFile.mkdirs()
    
    val oos = new ObjectOutputStream(new BufferedOutputStream(new FileOutputStream(file)))
    try {
      oos.writeObject(this)
      logInfo(s"[MERKLE] Saved tree to $filepath (${file.length()} bytes)")
    } finally {
      oos.close()
    }
  }
}

object MerkleTree extends Logging {
  
  def buildTree(data: Array[(Long, String)]): MerkleNode = {
    if (data.isEmpty) {
      // Empty partition is valid - return a special empty node with predictable hash
      logInfo("[MERKLE] Building tree from empty partition - using EmptyNode")
      return LeafNode(UnsafeStageMarkers.EMPTY_PARTITION_HASH, UnsafeStageMarkers.EMPTY_PARTITION_UID)
    }
    
    // Check for UNSAFE_STAGE marker
    if (data.length == 1 && data(0)._1 == UnsafeStageMarkers.UNSAFE_STAGE_UID) {
      logWarning("[MERKLE] Detected UNSAFE_STAGE marker - no safe transformations in stage")
      return LeafNode(UnsafeStageMarkers.UNSAFE_STAGE_HASH, UnsafeStageMarkers.UNSAFE_STAGE_UID)
    }
    
    val leaves: Array[MerkleNode] = data.map { case (uid, value) =>
      val hash = value.hashCode
      LeafNode(hash, uid): MerkleNode
    }
    
    buildTreeRecursive(leaves)(0)
  }
  
  @scala.annotation.tailrec
  private def buildTreeRecursive(nodes: Array[MerkleNode]): Array[MerkleNode] = {
    nodes match {
      case ns if ns.size <= 1 =>
        ns
      case ns =>
        val pairedNodes: Array[MerkleNode] = ns.grouped(2).map {
          case Array(a, b) =>
            val combinedHash = (a.hash + b.hash).hashCode
            InternalNode(combinedHash, a, b): MerkleNode
          case Array(a) =>
            val combinedHash = (a.hash + a.hash).hashCode
            InternalNode(combinedHash, a, a): MerkleNode
        }.toArray
        buildTreeRecursive(pairedNodes)
    }
  }
  
  def loadFromFile(filepath: String): MerkleTree = {
    val file = new File(filepath)
    if (!file.exists()) {
      throw new FileNotFoundException(s"Merkle tree file not found: $filepath")
    }
    
    val ois = new ObjectInputStream(new BufferedInputStream(new FileInputStream(file)))
    try {
      val tree = ois.readObject().asInstanceOf[MerkleTree]
      logInfo(s"[MERKLE] Loaded tree from $filepath (${tree.leafCount} leaves)")
      tree
    } finally {
      ois.close()
    }
  }
  
  def verify(tree1: MerkleTree, tree2: MerkleTree): Option[(Long, Int, Int)] = {
    if (tree1.root.hash == tree2.root.hash) {
      None
    } else {
      Some(findFirstDifference(tree1.root, tree2.root))
    }
  }
  
  private def findFirstDifference(node1: MerkleNode, node2: MerkleNode): (Long, Int, Int) = {
    (node1, node2) match {
      case (LeafNode(h1, id1), LeafNode(h2, id2)) if h1 != h2 =>
        if (id1 == id2) {
          // Normal case: same position, different values
          (id1, h1, h2)
        } else {
          // UID mismatch: potential swap attack, signal full task recomputation
          (-2L, h1, h2)
        }
      case (LeafNode(_, _), LeafNode(_, _)) =>
        (-1L, 0, 0)
      case (InternalNode(_, l1, r1), InternalNode(_, l2, r2)) =>
        if (l1.hash != l2.hash) {
          findFirstDifference(l1, l2)
        } else {
          findFirstDifference(r1, r2)
        }
      case _ =>
        (-1L, 0, 0)
    }
  }
  
  def buildFromFinalsFile(filepath: String, binary: Boolean): MerkleTree = {
    val file = new File(filepath)
    if (!file.exists()) {
      throw new FileNotFoundException(s"Finals file not found: $filepath")
    }
    
    val data = if (binary) {
      readBinaryFinalsFile(file)
    } else {
      readTextFinalsFile(file)
    }
    
    logInfo(s"[MERKLE] Building tree from ${data.length} elements in $filepath")
    new MerkleTree(data)
  }
  
  private def readBinaryFinalsFile(file: File): Array[(Long, String)] = {
    val dis = new DataInputStream(new BufferedInputStream(new FileInputStream(file)))
    val buffer = scala.collection.mutable.ArrayBuffer[(Long, String)]()
    
    try {
      while (dis.available() > 0) {
        val uid = dis.readLong()
        val length = dis.readInt()
        val bytes = new Array[Byte](length)
        dis.readFully(bytes)
        val value = new String(bytes, "UTF-8")
        buffer += ((uid, value))
      }
    } finally {
      dis.close()
    }
    
    buffer.toArray.sortBy(_._1)
  }
  
  private def readTextFinalsFile(file: File): Array[(Long, String)] = {
    val source = scala.io.Source.fromFile(file)
    try {
      source.getLines()
        .map { line =>
          val parts = line.split("\\|", 2)
          (parts(0).toLong, parts(1))
        }
        .toArray
        .sortBy(_._1)
    } finally {
      source.close()
    }
  }
}
