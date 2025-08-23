import os
from time import sleep

from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.action_chains import ActionChains
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import WebDriverWait

from . import sbi_enum, utils
from .mail import gmail

BASE_PATH = os.path.dirname(__file__)


class STOCK:
    order_types = {}

    def __init__(self, id=None, password=None, trading_pass=None) -> None:
        self.logger = utils.setup_logger()
        options = Options()
        options.add_argument(f"--user-data-dir={os.path.join(BASE_PATH, "profile")}")
        self.driver = webdriver.Chrome(options=options)
        self.driver.implicitly_wait(5)
        self.open()
        self.logger.debug("start initial login process.")
        self.login(id, password)
        self.trading_pass = trading_pass

    def __check_login(self, retry_count=0):
        """
        return False if newly logged in
        """
        # this take 10 sec.
        # if self.is_driver_available() is False:
        #     self.open()
        if self.is_logged_in() is False:
            if self.login(self.id, self.pa):
                return True
            elif retry_count < 5:
                # try opining selenium again
                self.driver.quit()
                self.__init__()
                self.open()
                return self.__check_login(retry_count + 1)
            else:
                print("Failed to check login state.")
                return False
        return True

    def is_driver_available(self):
        try:
            self.driver.current_url
            return True
        except Exception:
            print("driver is unavailable.")
            return False

    def open(self):
        self.driver.get("https://site1.sbisec.co.jp/ETGate/")

    def is_login_page(self):
        target_name = "ACT_login"
        try:
            login_ele = self.driver.find_element(By.NAME, target_name)
            if login_ele:
                return True
        except Exception:
            return False

    def __get_device_code_element(self):
        try:
            self.logger.debug("getting device element")
            eleemt = self.driver.find_element(By.NAME, "device_code")
            return eleemt
        except Exception:
            return None

    def is_logged_in(self) -> bool:
        try:
            self.driver.find_element(By.NAME, "user_password")
            return False
        except exceptions.NoSuchElementException:
            # If user_password is not found, it means logged in
            return True
        except Exception as e:
            print(f"error happened on is_logged_in: {e}")
            return False

    def _get_device_code(self):
        # If anyone want other source, need to overwrite the method
        return gmail.retrieve_sbi_device_code()

    def handle_otp(self, device_element) -> bool:
        try:
            self.logger.debug("start getting device code from gmail")
            if device_element is None:
                device_element = self.driver.find_element(By.NAME, "device_code")
            checkbox = self.driver.find_element(By.NAME, "device_string_checkbox")
            checkbox.click()
            device_code = self._get_device_code()
            if device_code:
                self.logger.debug("input device code")
                reg_device_element = self.driver.find_element(By.NAME, "ACT_deviceauth")
                device_element.send_keys(device_code)
                reg_device_element.click()
                return True
            else:
                self.logger.warning("device code not found.")
                return False
        except exceptions.NoSuchElementException as e:
            self.logger.error(e)
            return self.is_logged_in()
        except Exception as e:
            self.logger.error(e)
            return False

    def login(self, id: str, password: str) -> bool:
        try:
            id_ele = self.driver.find_element(By.NAME, "user_id")
            id_ele.send_keys(id)
            pa_ele = self.driver.find_element(By.NAME, "user_password")
            pa_ele.send_keys(password)
            log_ele = self.driver.find_element(By.NAME, "ACT_login")
            log_ele.click()
            device_element = self.__get_device_code_element()
            if device_element is None:
                self.logger.debug(
                    "no device element. check logged in or encountered an error."
                )
                if self.is_logged_in():
                    # store creds to utilize them when login life time end
                    self.logger.info("login is completed.")
                    self.id = id
                    self.pa = password
                    return True
                else:
                    self.logger.warning(
                        "Failed to login. Please check your id and password."
                    )
                    return False
            else:
                # wait to receive device code
                sleep(3)
                if self.handle_otp(device_element):
                    self.logger.debug("device code was available. check login state.")
                    if self.is_logged_in():
                        self.id = id
                        self.pa = password
                        return True
                    else:
                        self.logger.warning("Failed to loing. Please try again.")
                        return False
                else:
                    self.logger.warning("Failed to loing. Please try again.")
        except Exception as e:
            self.logger.error(e)
            return False

    def __open_symbol_page(self, symbol: str) -> bool:
        try:
            wait = WebDriverWait(self.driver, 5)
            form_ele = wait.until(
                EC.presence_of_element_located((By.ID, "brand-search-text"))
            )
            self.logger.debug("found an element to search symbol")
            form_ele.send_keys(symbol)
            form_ele.send_keys(Keys.ENTER)
            # If other symbol page already opened, sometimes wait.until pass before search result comes
            sleep(1)
            header_ele = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "head01"))
            )
            del wait
            # If there are some candidates or no candidate for a symbol text, search result page is shown
            if "検索結果" in header_ele.text:
                log_text = f"{symbol} returned candidates page on open_order_page"
                print(log_text)
                self.logger.warning(log_text)
                return False
            # If one candidate
            elif "国内株式" in header_ele.text:
                self.logger.debug("found a symbol")
                return True
            else:
                self.logger.warning("failed to load the symbol page")
                return False
        except Exception as e:
            self.logger.error(f"error happened on open_symbol_page {e}")
            return False

    def __transit_symbol_page_to_order_page(self, order_type) -> bool:
        result = False
        try:
            wait = WebDriverWait(self.driver, 20)
            header_ele = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "head01"))
            )
            # header_ele = self.driver.find_element(By.CLASS_NAME, "head01")
            if header_ele.text == "国内株式":
                candidate_eles = self.driver.find_elements(By.CLASS_NAME, order_type)
                target_txt = sbi_enum.symbol_page_to_order_element[order_type]
                for candidate in candidate_eles:
                    text_ele = candidate.find_element(By.CLASS_NAME, "fm01")
                    if text_ele:
                        if text_ele.text == target_txt:
                            text_ele.click()
                            result = True
                            break
                if result is False:
                    print(f"no elemnt for {order_type}")
            else:
                print("header is not found.")
            return result
        except Exception as e:
            print(f"error happend on transit_symbol_page_to_order_page {e}")
            return False

    def filling_order_page(self, amount, password, market=False, price=None):
        """order the symbol
            if market is True or price is None, order market buy/sell

        Args:
            amount (int): number of unit to order. Ex) If Unit 100, amount=3 means 300.
            password (str): password to order
            market (bool, optional): Option to order with market price. Defaults to False.
            price (float, optional): Price to order. Defaults to None.

        Returns:
            bool: suc or not
            str: error text. if succeed, None
        """
        target_class_name = "mtext"
        wait = WebDriverWait(self.driver, 10)
        mtexts_eles = wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, target_class_name))
        )
        if mtexts_eles:
            # input amount
            self.logger.debug("start filling to sell")
            mtexts_eles = self.driver.find_elements(By.CLASS_NAME, target_class_name)
            for ele in mtexts_eles:
                if "売買単位" in ele.text:
                    order_unit = int(ele.text.split("：")[1])
                    break
            target_name = "input_quantity"
            input_ele = self.driver.find_element(By.NAME, target_name)
            input_ele.clear()
            input_ele.send_keys(amount * order_unit)
            self.logger.debug("filled order amount")
            if market is True or price is None:
                market_rbutton = "nariyuki"
                market_rb_ele = self.driver.find_element(By.ID, market_rbutton)
                market_rb_ele.click()
                self.logger.debug("nariyuki order")
            else:
                # input price
                target_name = "input_price"
                price_ele = self.driver.find_element(By.NAME, target_name)
                price_ele.clear()
                price_ele.send_keys(price)
                self.logger.debug("filled order price")
            pwd3_input = "pwd3"
            pwd3_rb_ele = self.driver.find_element(By.ID, pwd3_input)
            pwd3_rb_ele.clear()
            pwd3_rb_ele.send_keys(password)
            self.logger.debug("filled order password")

            omit_cbox = "shouryaku"
            omit_cb_ele = self.driver.find_element(By.ID, omit_cbox)
            omit_cb_ele.click()
            self.logger.debug("enabled omit option")

            order_wo_conf = "botton2"
            order_btn_ele = self.driver.find_elements(By.ID, order_wo_conf)
            order_btn_ele[0].click()
            self.logger.debug("clicked to order")

            # check if order is completed
            target_name = "md-l-table-01"
            error_class = "fRed01"
            invalid_ele = self.driver.find_elements(By.CLASS_NAME, error_class)
            if invalid_ele:
                error_txt = invalid_ele[0].text
                if sbi_enum.ERROR_BUDGET_IS_SHORT in error_txt:
                    return False, sbi_enum.ERROR_BUDGET_IS_SHORT
                else:
                    return False, error_txt
            result_ele = self.driver.find_elements(By.CLASS_NAME, target_name)
            if result_ele:
                self.logger.info("order is completed.")
                return True, None

            error_txt = "failed to order with unkown issue."
            self.logger.error(error_txt)
            return False, error_txt
        else:
            error_txt = "can't find unit text"
            self.logger.error(f"failed to order: {error_txt}")
            return False, error_txt

    # Need to add try catch to avoid an error when handlers are called on different pages
    def __trade_page_handler(self, index: sbi_enum.TRADE_HEADER):
        """click header of trade page

        Args:
            index (TRADE_HEADER): index number of header position
        """
        if index.value >= 0 and index.value <= 9:
            self.__check_login()
            try:
                self.logger.debug("try to click header of trade page")
                # open trade page from header
                header_ele = self.driver.find_element(
                    By.CLASS_NAME, "slc-header-nav-lower-menu"
                )
                header_eles = header_ele.find_element(By.TAG_NAME, "ul").find_elements(
                    By.TAG_NAME, "li"
                )
                # 0: お知らせ, 2: ポートフォリト, 3: 取引
                header_eles[3].find_element(By.XPATH, ".//div/a").click()
            except Exception as e:
                self.logger.error(f"failed to open trade page: {e}")
                return False

            try:
                wait = WebDriverWait(self.driver, 10)
                table_ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "md-l-mainarea-01"))
                )
                tr_ele = table_ele.find_element(By.XPATH, ".//table/tbody/tr")
                td_eles = tr_ele.find_elements(By.XPATH, "td")
                href = td_eles[index.value].find_element(By.XPATH, "a")
                # sometimes header dropdown keep shown. close it by click top left
                ActionChains(self.driver).move_by_offset(0, 0).click().perform()
                sleep(0.3)
                href.click()
            except Exception as e:
                self.logger.error(f"failed to open {index} item: {e}")
                return False

            return True
        self.logger.error(f"index should be 0 to 9: {index}")
        return False

    def __header_bar_handler(self, option: sbi_enum.COMMON_HEADER):
        index = option.value

        if index >= 0 and index <= 13:
            self.logger.debug(f"open {index} of header bar")
            # header may exist on any page, so click it without page transition.
            target_class_name = "slc-global-nav-container"
            try:
                header_ele = self.driver.find_element(By.CLASS_NAME, target_class_name)
                lis = header_ele.find_element(By.TAG_NAME, "ul").find_elements(
                    By.TAG_NAME, "li"
                )
                lis[index].click()
                return True
            except Exception as e:
                self.logger.error(e)
                return False
        else:
            self.logger.error("index should be 0 to 13.")
            return False

    def __handle_symbol_page_header(self, index: int):
        if index <= 8 and index >= 0:
            wait = WebDriverWait(self.driver, 5)
            header_tr = wait.until(
                EC.presence_of_element_located((By.CLASS_NAME, "tab02T"))
            )
            self.logger.debug("found header for symbol information")
            first_tr = header_tr.find_element(By.XPATH, ".//table/tbody/tr")
            tds = first_tr.find_elements(By.TAG_NAME, "td")
            tds[index].click()
            if index == 0:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "kabuNowStatus"))
                )
            elif index == 1:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "newsMain01"))
                )
            elif index == 2:
                ele = wait.until(
                    EC.presence_of_element_located((By.ID, "CONTENTSAREA01"))
                )
            elif index == 3:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "mgt20"))
                )
                # case when review is not provided
                # wait.until(EC.presence_of_element_located((By.CLASS_NAME, "mtext-info-w")))
            elif index == 4:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "shikihouBox01"))
                )
            elif index == 5:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "summary_left"))
                )
            elif index == 6:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "stock_chart_disc"))
                )
            elif index == 7:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "accTbl01"))
                )
            elif index == 8:
                ele = wait.until(
                    EC.presence_of_element_located((By.CLASS_NAME, "Analysis"))
                )
            return True
        else:
            print("index should be 0 to 8")
            return False

    def open_position_page(self):
        return self.__trade_page_handler(sbi_enum.TRADE_HEADER.OWNED_STOCKS)

    def open_ordered_position_page(self):
        return self.__trade_page_handler(sbi_enum.TRADE_HEADER.ORDER_INQUIRY)

    def get_positions(self):
        if self.open_position_page():
            positions = []
            li_eles = self.driver.find_elements(By.CLASS_NAME, "seeds-table-row")
            self.logger.debug(f"found {len(li_eles)} symbols.")
            for li in li_eles:
                position = {}
                tds = li.find_elements(By.XPATH, "div")
                # get symbol name
                name_ele = tds[0].find_element(By.XPATH, ".//div/a")
                symbol_name = name_ele.text
                self.logger.debug(f"got symbol name: {symbol_name}")
                symbol_name = symbol_name.replace(" ", "")
                # get index number of the symbol
                index_ele = tds[0].find_element(By.XPATH, ".//div/div/div")
                symbol_index = index_ele.text
                try:
                    symbol_index = int(symbol_index)
                except Exception as e:
                    self.logger.error(f"failed to cast symbol index: {e}")

                # get holding number
                holding_number_ele = tds[1].find_element(
                    By.XPATH, ".//div/div[1]/div[1]"
                )
                total_holding_number = holding_number_ele.text
                ordering_number_ele = tds[1].find_element(
                    By.XPATH, ".//div/div[2]/div[1]"
                )
                ordering_number = ordering_number_ele.text
                self.logger.debug(
                    f"got holding info: {total_holding_number}, {ordering_number}"
                )
                ordering_number = ordering_number.replace("(", "")
                holding_number = None
                try:
                    holding_number = int(total_holding_number)
                    ordering_number = int(ordering_number)
                    holding_number -= ordering_number
                except Exception as e:
                    self.logger.error(f"failed to cast holding numbers: {e}")

                # get prices
                price_ele = tds[2].find_element(By.XPATH, ".//div/div[1]/div[1]")
                bought_price = price_ele.text
                current_price_ele = tds[2].find_element(
                    By.XPATH, ".//div/div[2]/div[1]"
                )
                current_price = current_price_ele.text
                self.logger.debug(f"got prices: {bought_price}")
                try:
                    bought_price = bought_price.replace(",", "")
                    bought_price = float(bought_price)
                    current_price = current_price.replace(",", "")
                    current_price = float(current_price)
                except Exception as e:
                    self.logger.error(f"failed to convert prices: {e}")

                links_container = tds[5].find_element(By.XPATH, ".//div/div")
                links = links_container.find_elements(By.XPATH, "a")
                buy_ele = links[0]
                sell_ele = links[1]
                self.logger.debug("got links")

                position["symbol_name"] = symbol_name
                position["symbol_index"] = symbol_index
                position["holding"] = holding_number
                position["bought_price"] = bought_price
                position["current_price"] = current_price
                position["buy_link"] = buy_ele
                position["sell_link"] = sell_ele

                positions.append(position)
            return positions
        else:
            return []

    def get_rating(self, symbol: str) -> dict:
        """
        Args:
            symbol (str): company name or index

        Returns:
            dict: key: number of star, value: number of traders who advocate the key
        """
        try:
            self.logger.debug("start getting rate")
            if self.__open_symbol_page(symbol):
                if self.__handle_symbol_page_header(3):
                    tr_eles = self.driver.find_elements(By.CLASS_NAME, "vaT")
                    if len(tr_eles) != 8:
                        if len(tr_eles) == 3:
                            print(f"rating is not provided for {symbol}")
                            return {}
                        print(
                            f"number of element doesn't match with assumption on {symbol}. HP may be updated"
                        )
                        return {}
                    rating = {}
                    for index in range(3, 8):
                        tds = tr_eles[index].find_elements(By.XPATH, "td")
                        rating[8 - index] = int(tds[2].text)
                    return rating
                else:
                    print("couldn't click header on sygnal page.")
                    return {}
            else:
                print("failed to open symbol page.")
                return {}

        except Exception as e:
            self.logger.error(f"error happened on get_rating: {e}")

    def get_ratings(self, symbols):
        """
        Args:
            symbols (list): list of company name or index

        Returns:
            dict: key: sygnal, value: rating. If not provided, {}
        """
        ratings = {}
        if len(symbols) > 0:
            for symbol in symbols:
                self.logger.info("start getting rating")
                rating = self.get_rating(symbol)
                if type(rating) is dict:
                    ratings[symbol] = rating
                    self.logger.debug("added rating")
                else:
                    self.logger.debug(f"rating is not available for {symbol}")
        return ratings

    def buy_order(self, symbol: str, amount: int, order_price: float = None):
        """

        Args:
            symbol (str): symbol to order. Allows if search result shows a page of symbol.
            amount (int): amount of order. try to buy with (amount * unit) * order_price
            order_price (float, optional): price to order. Defaults to None.

        Returns:
            bool: Return True if order is completed
        """
        self.__check_login()
        if self.__open_symbol_page(symbol):
            if self.__transit_symbol_page_to_order_page(sbi_enum.buy):
                self.filling_order_page(
                    amount, password=self.trading_pass, price=order_price
                )
                return True
            else:
                print("Failed to transit to order page")
                return False
        else:
            print("failed to open symbol page")
            return False

    def buy_less_than_unit(self, symbol, amount: int):
        raise Exception("Not implemented")

    def sell_to_close_buy_order(self, symbol, amount, order_price=None):
        """
        Sell to close

        Args:
            symbol (str): symbol name or id
            amount (int): amount of order. try to buy with (amount * unit) * order_price
            order_price (float, optional): price to order. Defaults to None.

        Returns:
            _type_: _description_
        """
        positions = self.get_positions()
        if len(positions) > 0:
            is_clicked = False
            for position in positions:
                name = position["symbol_name"]
                index = position["symbol_index"]
                if symbol == name or symbol == str(index) or symbol == int(index):
                    position["sell_link"].click()
                    self.logger.debug("found symbol in position page.")
                    is_clicked = True
                    break
            if is_clicked:
                return self.filling_order_page(
                    amount, self.trading_pass, price=order_price
                )
            else:
                self.logger.warning(f"{symbol} is not found on symbol column.")
                return False, None
        else:
            self.logger.info("No position available")
            return False, None

    def get_orders(self) -> list:
        target_row_class_name = "md-l-tr-01"
        wait = WebDriverWait(self.driver, 5)
        wait.until(
            EC.presence_of_element_located((By.CLASS_NAME, target_row_class_name))
        )
        trs = self.driver.find_elements(By.CLASS_NAME, target_row_class_name)
        orders = []
        for index in range(0, len(trs), 2):
            order = {}
            tds = trs[index].find_elements(By.XPATH, "td")
            names = tds[3].text.split(" ")
            symbol_name = names[0]
            symbol_index = int(names[2])

            links = tds[4].find_elements(By.XPATH, "a")
            cancel_ele = links[0]
            modify_ele = links[1]

            order["symbol_name"] = symbol_name
            order["symbol_index"] = symbol_index
            order["cancel_link"] = cancel_ele
            order["modify_link"] = modify_ele
            orders.append(order)

        return orders

    def __click_cancel_on_cancel_page(self, password, do_print=False):
        wait = WebDriverWait(self.driver, 5)
        try:
            pass_ele = wait.until(EC.presence_of_element_located((By.ID, "pwd3")))
        except Exception:
            print("password field is not found within timeout")
            return False
        if do_print:
            tds = self.driver.find_elements(By.CLASS_NAME, "vaM")
            for index in range(0, len(tds), 2):
                print(f"{tds[index].text}:{tds[index+1].text}")
        pass_ele.send_keys(password)
        try:
            self.driver.find_element(By.NAME, "ACT_place").click()
            return True
        except Exception:
            print("cancel button is not found")
            return False

    def cancel_order(self, symbol):
        if self.open_ordered_position_page():
            orders = self.get_orders()
            if len(orders) > 0:
                is_clicked = False
                for order in orders:
                    if (
                        (order["symbol_name"] == symbol)
                        or (str(order["symbol_index"]) == symbol)
                        or (int(order["symbol_index"]) == symbol)
                    ):
                        order["cancel_link"].click()
                        is_clicked = True
                        break
                if is_clicked:
                    return self.__click_cancel_on_cancel_page(self.trading_pass)
                else:
                    print(f"{symbol} is not found on symbol column.")
            else:
                print("No orders available.")
        return False

    def get_available_budget(self) -> int:
        self.logger.debug("start getting available buget")
        if self.__check_login():
            self.logger.debug("logged in state")
            if self.__header_bar_handler(sbi_enum.COMMON_HEADER.HOME):
                div = self.driver.find_element(By.CLASS_NAME, "seeds-list-form-lg")
                buget_text_element = div.find_element(
                    By.XPATH, ".//div/div[1]/div[3]/span/div/div[1]"
                )
                self.logger.debug(f"retrieved {buget_text_element.text}. Try parse it.")
                budget_txt = buget_text_element.text.replace(",", "")
                try:
                    return int(budget_txt)
                except Exception as e:
                    print(f"unable to convert budget {budget_txt} to int: {e}")
                    return None
            else:
                self.logger.error("failed to open header bar[0]")
                return None
        else:
            return None

    def __del__(self):
        if hasattr(self, "driver"):
            self.driver.quit()
