"""
Модуль для конвертации XML в DataFrame.

Конвертирует XML-файлы со вложенной структурой в Pandas DataFrame.
Использует xml.dom.minidom для парсинга.
"""
__author__ = "Beshentsev-AAl"
__date__ = "06.05.2024"
__version__ = "0.6"

from typing import Optional, TYPE_CHECKING
from xml.dom import minidom, Node
from collections import Counter
import logging
import os

import pandas as pd

if TYPE_CHECKING:
    from xml.dom.minidom import Element

logger = logging.getLogger(__name__)


class XML2DF:
    """
    Класс для конвертации XML со вложенной структурой в Pandas DataFrame.

    Пример использования:
        x2d = XML2DF()
        df = x2d.get_df_from_xml("data.xml")
    """

    def __init__(self) -> None:
        self._xml_rows: list[dict[str, str]] = []

    def _build_path(self, parent_path: str, node_name: str) -> str:
        """
        Строит полный путь к узлу.

        Args:
            parent_path: Путь к родительскому узлу.
            node_name: Имя текущего узла.

        Returns:
            Путь к узлу с разделителем '|'.
        """
        if parent_path:
            return f"{parent_path}|{node_name}"
        return node_name

    def _get_text_content(self, node: Node) -> Optional[str]:
        """
        Возвращает текстовое содержимое узла.

        Args:
            node: Узел XML.

        Returns:
            Текст или None, если текст отсутствует.
        """
        text_parts: list[str] = []
        for child in node.childNodes:  # type: ignore[attr-defined]
            if child.nodeType == Node.TEXT_NODE and child.nodeValue:  # type: ignore[attr-defined]
                stripped = child.nodeValue.strip()  # type: ignore[attr-defined]
                if stripped:
                    text_parts.append(stripped)
        return " ".join(text_parts) if text_parts else None

    def _find_anchor_tag(self, node: Node) -> Optional[str]:
        """
        Ищет самый глубокий тег, повторяющийся на одном уровне.

        Args:
            node: Корневой узел документа.

        Returns:
            Имя якорного тега или None если не найден.
        """
        best_tag = None
        best_depth = -1

        def search(current: Node, depth: int) -> None:
            nonlocal best_tag, best_depth

            counts: Counter = Counter()
            children = []
            for child in current.childNodes:  # type: ignore[attr-defined]
                if child.nodeType == Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                    counts[child.nodeName] += 1  # type: ignore[attr-defined]
                    children.append(child)

            repeating = [tag for tag, n in counts.items() if n > 1]
            if repeating and depth > best_depth:
                best_depth = depth
                best_tag = max(repeating, key=lambda t: counts[t])

            for child in children:
                search(child, depth + 1)

        search(node, 0)
        return best_tag

    def _collect_node_data(
        self,
        node: Node,
        row: dict[str, str],
        path: str,
    ) -> None:
        """
        Рекурсивно обходит поддерево узла и заполняет строку данными.

        Args:
            node: Текущий узел XML.
            row: Словарь строки, который заполняем.
            path: Путь к текущему узлу.
        """
        for child in node.childNodes:  # type: ignore[attr-defined]
            if child.nodeType != Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                continue

            child_path = self._build_path(path, child.nodeName)  # type: ignore[attr-defined]

            # сначала текст — чтобы атрибуты не затёрли
            text = self._get_text_content(child)
            if text:
                row[child_path] = text

            # потом атрибуты
            for attr_name, attr_value in child.attributes.items():  # type: ignore[attr-defined]
                row[self._build_path(child_path, attr_name)] = attr_value

            self._collect_node_data(child, row, child_path)

    def _contains_anchor(self, node: Node, anchor_tag: str) -> bool:
        """
        Проверяет, содержит ли узел якорный тег на любом уровне.

        Args:
            node: Узел для проверки.
            anchor_tag: Имя якорного тега.

        Returns:
            True если якорный тег найден.
        """
        for child in node.childNodes:  # type: ignore[attr-defined]
            if child.nodeType != Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                continue
            if child.nodeName == anchor_tag:  # type: ignore[attr-defined]
                return True
            if self._contains_anchor(child, anchor_tag):
                return True
        return False

    def _build_ancestor_context(self, node: Node, anchor_tag: str) -> dict[str, str]:
        """
        Собирает данные прямой цепочки предков якорного узла.

        Args:
            node: Якорный узел.
            anchor_tag: Имя якорного тега.

        Returns:
            Словарь с данными предков.
        """
        ancestors = []
        parent = node.parentNode  # type: ignore[attr-defined]
        while parent and parent.nodeType == Node.ELEMENT_NODE:  # type: ignore[attr-defined]
            ancestors.append(parent)
            parent = parent.parentNode  # type: ignore[attr-defined]

        context: dict[str, str] = {}

        for ancestor in reversed(ancestors):
            ancestor_path = ancestor.nodeName  # type: ignore[attr-defined]

            # собираем атрибуты предка
            for attr_name, attr_value in ancestor.attributes.items():  # type: ignore[attr-defined]
                context[self._build_path(ancestor_path, attr_name)] = attr_value

            # собираем детей предка — пропускаем якорные и ведущие к якорному
            for child in ancestor.childNodes:  # type: ignore[attr-defined]
                if child.nodeType != Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                    continue
                if child.nodeName == anchor_tag:  # type: ignore[attr-defined]
                    continue
                if self._contains_anchor(child, anchor_tag):
                    continue

                child_path = self._build_path(ancestor_path, child.nodeName)  # type: ignore[attr-defined]

                for attr_name, attr_value in child.attributes.items():  # type: ignore[attr-defined]
                    context[self._build_path(child_path, attr_name)] = attr_value

                text = self._get_text_content(child)
                if text:
                    context[child_path] = text

                self._collect_node_data(child, context, child_path)

        return context

    def _parse_by_tag(self, doc: minidom.Document, tag_name: str) -> None:
        """
        Парсит документ по якорному тегу.

        Args:
            doc: XML документ.
            tag_name: Якорный тег — каждое вхождение становится строкой.
        """
        for node in doc.getElementsByTagName(tag_name):
            row = self._build_ancestor_context(node, tag_name)

            # строим полный путь до якорного тега
            parts = []
            current = node
            while current and current.nodeType == Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                parts.append(current.nodeName)  # type: ignore[attr-defined]
                current = current.parentNode  # type: ignore[attr-defined]
            anchor_path = "|".join(reversed(parts))

            # собираем атрибуты якорного тега с полным путём
            for attr_name, attr_value in node.attributes.items():  # type: ignore[attr-defined]
                row[self._build_path(anchor_path, attr_name)] = attr_value

            # собираем текст якорного тега
            text = self._get_text_content(node)
            if text:
                row[anchor_path] = text

            # собираем данные внутри якорного тега с полным путём
            self._collect_node_data(node, row, anchor_path)

            self._xml_rows.append(row)

    def get_df_from_xml(
        self,
        xml_path: str,
        drop_nan_cols: bool = True,
    ) -> pd.DataFrame:
        """
        Парсит XML и возвращает DataFrame.

        Автоматически определяет якорный тег — самый глубокий повторяющийся элемент.

        Args:
            xml_path: Путь к XML-файлу.
            drop_nan_cols: Удалить столбцы, где все значения пустые.

        Returns:
            DataFrame с данными из XML.

        Raises:
            FileNotFoundError: Если файл не найден.
            ValueError: Если XML невалиден или пуст.
            RuntimeError: При других ошибках парсинга.
        """
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"XML файл не найден: {xml_path}")

        self._xml_rows = []

        try:
            doc = minidom.parse(xml_path)
        except Exception as exc:
            raise ValueError(f"Ошибка парсинга XML: {exc}") from exc

        try:
            root = doc.documentElement
            if root is None:
                raise ValueError("XML документ пуст")

            # определяем якорный тег автоматически
            tag_name = self._find_anchor_tag(root)
            if not tag_name:
                raise ValueError("Не удалось найти повторяющийся тег в XML")

            self._parse_by_tag(doc, tag_name)

            if not self._xml_rows:
                return pd.DataFrame()

            df = pd.DataFrame(self._xml_rows)

            if drop_nan_cols:
                df = df.dropna(axis=1, how="all")

            return df

        except Exception as exc:
            raise RuntimeError(f"Ошибка при конвертации XML в DataFrame: {exc}") from exc

    def get_tree_nodes(self, xml_path: str) -> list[dict]:
        """
        Строит дерево узлов из XML для отображения в streamlit_tree_select.

        Args:
            xml_path: Путь к XML-файлу.

        Returns:
            Список узлов в формате streamlit_tree_select.
        """
        if not os.path.exists(xml_path):
            raise FileNotFoundError(f"XML файл не найден: {xml_path}")

        try:
            doc = minidom.parse(xml_path)
        except Exception as exc:
            raise ValueError(f"Ошибка парсинга XML: {exc}") from exc

        def build_node(node: Node, path: str) -> dict:
            current_path = self._build_path(path, node.nodeName)  # type: ignore[attr-defined]
            result = {
                "label": node.nodeName,  # type: ignore[attr-defined]
                "value": current_path,
            }

            children = []

            # добавляем текстовое значение как отдельный узел если есть и атрибуты и текст
            text = self._get_text_content(node)
            has_attrs = node.attributes and node.attributes.length > 0  # type: ignore[attr-defined]
            if text and has_attrs:
                children.append({
                    "label": "#value",
                    "value": f"{current_path}#value",
                })

            # добавляем атрибуты как дочерние узлы
            for attr_name in node.attributes.keys():  # type: ignore[attr-defined]
                attr_path = self._build_path(current_path, attr_name)
                children.append({
                    "label": f"@{attr_name}",
                    "value": attr_path,
                })

            # рекурсивно добавляем дочерние элементы
            seen = set()
            for child in node.childNodes:  # type: ignore[attr-defined]
                if child.nodeType != Node.ELEMENT_NODE:  # type: ignore[attr-defined]
                    continue
                if child.nodeName in seen:  # type: ignore[attr-defined]
                    continue
                seen.add(child.nodeName)  # type: ignore[attr-defined]
                children.append(build_node(child, current_path))

            if children:
                result["children"] = children

            return result

        root = doc.documentElement
        if root is None:
            raise ValueError("XML документ пуст")

        return [build_node(root, "")]