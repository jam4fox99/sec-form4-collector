"""
Form 4 XML Parser for extracting transaction data from SEC filings.
Handles both new XML format and legacy text formats.
"""
import xml.etree.ElementTree as ET
import logging
import re
from datetime import datetime, date
from typing import Dict, List, Optional, Union, Any
from dataclasses import dataclass
from decimal import Decimal, InvalidOperation

logger = logging.getLogger(__name__)


@dataclass
class TransactionData:
    """Structured transaction data from Form 4"""
    transaction_date: Optional[date]
    transaction_code: Optional[str]
    shares: Optional[Decimal]
    price_per_share: Optional[Decimal]
    total_value: Optional[Decimal]
    shares_owned_after: Optional[Decimal]
    is_direct: Optional[bool]
    security_title: Optional[str]
    transaction_type: str  # 'common' or 'derivative'
    notes: Optional[str]


@dataclass
class ReportingOwner:
    """Information about the reporting owner"""
    cik: Optional[str]
    name: Optional[str]
    address: Optional[str]
    relationship: Optional[str]
    is_officer: bool = False
    is_director: bool = False
    is_ten_percent_owner: bool = False
    officer_title: Optional[str] = None


@dataclass
class IssuerInfo:
    """Information about the issuer (company)"""
    cik: Optional[str]
    name: Optional[str]
    trading_symbol: Optional[str]


@dataclass
class ParsedForm4:
    """Complete parsed Form 4 data"""
    issuer: IssuerInfo
    reporting_owner: ReportingOwner
    non_derivative_transactions: List[TransactionData]
    derivative_transactions: List[TransactionData]
    footnotes: Dict[str, str]
    document_type: str
    period_of_report: Optional[date]
    date_of_original_submission: Optional[date]


class Form4Parser:
    """
    Parses Form 4 XML files to extract transaction data.
    Handles both modern XML format and legacy text formats.
    """
    
    def __init__(self):
        """Initialize Form 4 parser"""
        self.footnotes = {}
        logger.info("Form 4 parser initialized")
    
    def parse_form4_xml(self, xml_content: str) -> Optional[ParsedForm4]:
        """
        Parse Form 4 XML content and extract structured data.
        
        Args:
            xml_content: Raw XML content from SEC filing
            
        Returns:
            ParsedForm4 object with extracted data, or None if parsing fails
        """
        try:
            # Clean the XML content
            xml_content = self._clean_xml_content(xml_content)
            
            # Check if this is XML format
            if '<ownershipDocument>' in xml_content:
                return self._parse_xml_format(xml_content)
            elif '<DOCUMENT>' in xml_content:
                return self._parse_legacy_format(xml_content)
            else:
                logger.warning("Unknown Form 4 format")
                return None
                
        except Exception as e:
            logger.error(f"Error parsing Form 4: {e}")
            return None
    
    def _clean_xml_content(self, xml_content: str) -> str:
        """
        Clean XML content to handle common issues.
        
        Args:
            xml_content: Raw XML content
            
        Returns:
            Cleaned XML content
        """
        # Remove XML declaration if present
        if xml_content.startswith('<?xml'):
            xml_content = xml_content[xml_content.find('?>') + 2:]
        
        # Remove any control characters
        xml_content = re.sub(r'[\x00-\x08\x0B\x0C\x0E-\x1F\x7F]', '', xml_content)
        
        # Handle common encoding issues
        xml_content = xml_content.replace('&', '&amp;')
        xml_content = xml_content.replace('&amp;amp;', '&amp;')
        xml_content = xml_content.replace('&amp;lt;', '&lt;')
        xml_content = xml_content.replace('&amp;gt;', '&gt;')
        
        return xml_content.strip()
    
    def _parse_xml_format(self, xml_content: str) -> Optional[ParsedForm4]:
        """
        Parse modern XML format Form 4.
        
        Args:
            xml_content: XML content
            
        Returns:
            ParsedForm4 object
        """
        try:
            root = ET.fromstring(xml_content)
            
            # Parse issuer information
            issuer = self._parse_issuer_info(root)
            
            # Parse reporting owner information
            reporting_owner = self._parse_reporting_owner(root)
            
            # Parse footnotes
            footnotes = self._parse_footnotes(root)
            
            # Parse document info
            document_type = self._get_text_value(root, './/documentType')
            period_of_report = self._parse_date(self._get_text_value(root, './/periodOfReport'))
            date_of_original_submission = self._parse_date(
                self._get_text_value(root, './/dateOfOriginalSubmission')
            )
            
            # Parse transactions
            non_derivative_transactions = self._parse_non_derivative_transactions(root)
            derivative_transactions = self._parse_derivative_transactions(root)
            
            return ParsedForm4(
                issuer=issuer,
                reporting_owner=reporting_owner,
                non_derivative_transactions=non_derivative_transactions,
                derivative_transactions=derivative_transactions,
                footnotes=footnotes,
                document_type=document_type or 'Form 4',
                period_of_report=period_of_report,
                date_of_original_submission=date_of_original_submission
            )
            
        except ET.ParseError as e:
            logger.error(f"XML parse error: {e}")
            return None
        except Exception as e:
            logger.error(f"Error parsing XML format: {e}")
            return None
    
    def _parse_issuer_info(self, root: ET.Element) -> IssuerInfo:
        """Parse issuer information from XML"""
        issuer_elem = root.find('.//issuer')
        if issuer_elem is None:
            return IssuerInfo(cik=None, name=None, trading_symbol=None)
        
        cik = self._get_text_value(issuer_elem, './/issuerCik')
        name = self._get_text_value(issuer_elem, './/issuerName')
        trading_symbol = self._get_text_value(issuer_elem, './/issuerTradingSymbol')
        
        return IssuerInfo(cik=cik, name=name, trading_symbol=trading_symbol)
    
    def _parse_reporting_owner(self, root: ET.Element) -> ReportingOwner:
        """Parse reporting owner information from XML"""
        owner_elem = root.find('.//reportingOwner')
        if owner_elem is None:
            return ReportingOwner(cik=None, name=None, address=None, relationship=None)
        
        # Owner ID
        owner_id = owner_elem.find('.//reportingOwnerId')
        cik = self._get_text_value(owner_id, './/rptOwnerCik') if owner_id else None
        name = self._get_text_value(owner_id, './/rptOwnerName') if owner_id else None
        
        # Address
        address_elem = owner_elem.find('.//reportingOwnerAddress')
        address = None
        if address_elem is not None:
            address_parts = []
            for field in ['rptOwnerStreet1', 'rptOwnerStreet2', 'rptOwnerCity', 
                         'rptOwnerState', 'rptOwnerZipCode']:
                value = self._get_text_value(address_elem, f'.//{field}')
                if value:
                    address_parts.append(value)
            address = ', '.join(address_parts) if address_parts else None
        
        # Relationship
        relationship_elem = owner_elem.find('.//reportingOwnerRelationship')
        is_officer = False
        is_director = False
        is_ten_percent_owner = False
        officer_title = None
        
        if relationship_elem is not None:
            is_officer = self._get_text_value(relationship_elem, './/isOfficer') == '1'
            is_director = self._get_text_value(relationship_elem, './/isDirector') == '1'
            is_ten_percent_owner = self._get_text_value(relationship_elem, './/isTenPercentOwner') == '1'
            officer_title = self._get_text_value(relationship_elem, './/officerTitle')
        
        # Build relationship string
        relationships = []
        if is_officer:
            relationships.append(f"Officer ({officer_title})" if officer_title else "Officer")
        if is_director:
            relationships.append("Director")
        if is_ten_percent_owner:
            relationships.append("10% Owner")
        
        relationship_str = ', '.join(relationships) if relationships else None
        
        return ReportingOwner(
            cik=cik,
            name=name,
            address=address,
            relationship=relationship_str,
            is_officer=is_officer,
            is_director=is_director,
            is_ten_percent_owner=is_ten_percent_owner,
            officer_title=officer_title
        )
    
    def _parse_footnotes(self, root: ET.Element) -> Dict[str, str]:
        """Parse footnotes from XML"""
        footnotes = {}
        footnote_elems = root.findall('.//footnote')
        
        for footnote in footnote_elems:
            footnote_id = footnote.get('id')
            footnote_text = footnote.text
            if footnote_id and footnote_text:
                footnotes[footnote_id] = footnote_text.strip()
        
        return footnotes
    
    def _parse_non_derivative_transactions(self, root: ET.Element) -> List[TransactionData]:
        """Parse non-derivative transactions (Table I)"""
        transactions = []
        
        # Find all non-derivative transactions
        transaction_elems = root.findall('.//nonDerivativeTransaction')
        
        for trans_elem in transaction_elems:
            transaction = self._parse_transaction_element(trans_elem, 'common')
            if transaction:
                transactions.append(transaction)
        
        return transactions
    
    def _parse_derivative_transactions(self, root: ET.Element) -> List[TransactionData]:
        """Parse derivative transactions (Table II)"""
        transactions = []
        
        # Find all derivative transactions
        transaction_elems = root.findall('.//derivativeTransaction')
        
        for trans_elem in transaction_elems:
            transaction = self._parse_transaction_element(trans_elem, 'derivative')
            if transaction:
                transactions.append(transaction)
        
        return transactions
    
    def _parse_transaction_element(self, trans_elem: ET.Element, 
                                 transaction_type: str) -> Optional[TransactionData]:
        """Parse individual transaction element"""
        try:
            # Security title
            security_title = self._get_text_value(trans_elem, './/securityTitle/value')
            
            # Transaction date
            trans_date_str = self._get_text_value(trans_elem, './/transactionDate/value')
            transaction_date = self._parse_date(trans_date_str)
            
            # Transaction code
            transaction_code = self._get_text_value(trans_elem, './/transactionCode')
            
            # Shares and price
            shares_str = self._get_text_value(trans_elem, './/transactionShares/value')
            price_str = self._get_text_value(trans_elem, './/transactionPricePerShare/value')
            
            shares = self._parse_decimal(shares_str)
            price_per_share = self._parse_decimal(price_str)
            
            # Calculate total value
            total_value = None
            if shares and price_per_share:
                total_value = shares * price_per_share
            
            # Shares owned after transaction
            shares_owned_after_str = self._get_text_value(trans_elem, './/sharesOwnedFollowingTransaction/value')
            shares_owned_after = self._parse_decimal(shares_owned_after_str)
            
            # Direct or indirect ownership
            ownership_nature = self._get_text_value(trans_elem, './/directOrIndirectOwnership/value')
            is_direct = ownership_nature == 'D' if ownership_nature else None
            
            # Notes (from footnotes)
            notes = self._extract_footnote_references(trans_elem)
            
            return TransactionData(
                transaction_date=transaction_date,
                transaction_code=transaction_code,
                shares=shares,
                price_per_share=price_per_share,
                total_value=total_value,
                shares_owned_after=shares_owned_after,
                is_direct=is_direct,
                security_title=security_title,
                transaction_type=transaction_type,
                notes=notes
            )
            
        except Exception as e:
            logger.error(f"Error parsing transaction element: {e}")
            return None
    
    def _parse_legacy_format(self, content: str) -> Optional[ParsedForm4]:
        """
        Parse legacy text format Form 4.
        
        Args:
            content: Legacy format content
            
        Returns:
            ParsedForm4 object
        """
        # This is a simplified parser for legacy formats
        # In practice, you'd need more sophisticated parsing
        logger.warning("Legacy format parsing not fully implemented")
        return None
    
    def _get_text_value(self, element: Optional[ET.Element], path: str) -> Optional[str]:
        """Safely get text value from XML element"""
        if element is None:
            return None
        
        found_elem = element.find(path)
        if found_elem is not None and found_elem.text:
            return found_elem.text.strip()
        return None
    
    def _parse_date(self, date_str: Optional[str]) -> Optional[date]:
        """Parse date string to date object"""
        if not date_str:
            return None
        
        # Try different date formats
        formats = ['%Y-%m-%d', '%m/%d/%Y', '%m-%d-%Y']
        
        for fmt in formats:
            try:
                return datetime.strptime(date_str, fmt).date()
            except ValueError:
                continue
        
        logger.warning(f"Could not parse date: {date_str}")
        return None
    
    def _parse_decimal(self, value_str: Optional[str]) -> Optional[Decimal]:
        """Parse decimal value from string"""
        if not value_str:
            return None
        
        try:
            # Remove commas and other formatting
            clean_value = re.sub(r'[,$\s]', '', value_str)
            return Decimal(clean_value)
        except (InvalidOperation, ValueError):
            logger.warning(f"Could not parse decimal: {value_str}")
            return None
    
    def _extract_footnote_references(self, element: ET.Element) -> Optional[str]:
        """Extract footnote references from element"""
        footnote_refs = []
        
        # Look for footnote references
        for attr_name, attr_value in element.attrib.items():
            if attr_name.endswith('FootnoteId'):
                footnote_refs.append(attr_value)
        
        # Also check child elements
        for child in element.iter():
            for attr_name, attr_value in child.attrib.items():
                if attr_name.endswith('FootnoteId'):
                    footnote_refs.append(attr_value)
        
        if footnote_refs:
            footnote_texts = []
            for ref in footnote_refs:
                if ref in self.footnotes:
                    footnote_texts.append(f"({ref}) {self.footnotes[ref]}")
            return '; '.join(footnote_texts) if footnote_texts else None
        
        return None
    
    def extract_transactions(self, parsed_form4: ParsedForm4) -> List[Dict[str, Any]]:
        """
        Convert parsed Form 4 data to transaction records for database storage.
        
        Args:
            parsed_form4: Parsed Form 4 data
            
        Returns:
            List of transaction dictionaries
        """
        transactions = []
        
        # Process non-derivative transactions
        for trans in parsed_form4.non_derivative_transactions:
            transaction_dict = {
                'transaction_date': trans.transaction_date,
                'transaction_code': trans.transaction_code,
                'shares': trans.shares,
                'price_per_share': trans.price_per_share,
                'total_value': trans.total_value,
                'shares_owned_after': trans.shares_owned_after,
                'is_direct': trans.is_direct,
                'transaction_type': trans.transaction_type,
                'security_title': trans.security_title,
                'notes': trans.notes
            }
            transactions.append(transaction_dict)
        
        # Process derivative transactions
        for trans in parsed_form4.derivative_transactions:
            transaction_dict = {
                'transaction_date': trans.transaction_date,
                'transaction_code': trans.transaction_code,
                'shares': trans.shares,
                'price_per_share': trans.price_per_share,
                'total_value': trans.total_value,
                'shares_owned_after': trans.shares_owned_after,
                'is_direct': trans.is_direct,
                'transaction_type': trans.transaction_type,
                'security_title': trans.security_title,
                'notes': trans.notes
            }
            transactions.append(transaction_dict)
        
        return transactions
    
    def get_transaction_summary(self, parsed_form4: ParsedForm4) -> Dict[str, Any]:
        """
        Get summary statistics for the parsed Form 4.
        
        Args:
            parsed_form4: Parsed Form 4 data
            
        Returns:
            Dictionary with summary information
        """
        all_transactions = (parsed_form4.non_derivative_transactions + 
                          parsed_form4.derivative_transactions)
        
        if not all_transactions:
            return {
                'total_transactions': 0,
                'total_shares': 0,
                'total_value': 0,
                'transaction_codes': []
            }
        
        total_shares = sum(t.shares for t in all_transactions if t.shares)
        total_value = sum(t.total_value for t in all_transactions if t.total_value)
        transaction_codes = list(set(t.transaction_code for t in all_transactions 
                                   if t.transaction_code))
        
        return {
            'total_transactions': len(all_transactions),
            'total_shares': total_shares,
            'total_value': total_value,
            'transaction_codes': transaction_codes,
            'issuer_name': parsed_form4.issuer.name,
            'reporting_owner': parsed_form4.reporting_owner.name,
            'period_of_report': parsed_form4.period_of_report
        } 