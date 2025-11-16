import logging
import os
import re
import time

import pandas as pd
from selenium import webdriver
from selenium.common import exceptions
from selenium.webdriver.chrome.options import Options
from selenium.webdriver.common.by import By
from selenium.webdriver.common.keys import Keys
from selenium.webdriver.support import expected_conditions as EC
from selenium.webdriver.support.ui import Select, WebDriverWait

from . import auth, utils
from .sbi_enum import *

logger = logging.getLogger(__name__)

ADVANCED_OPTION_LIMIT = 10

class StatefulScreening:

    def __init__(self, id, password, driver: webdriver.Chrome = None):
        if driver is None:
            options = Options()
            options.add_argument(f"--user-data-dir={os.path.join(utils.BASE_PATH, "profile")}")
            options.add_argument("user-agent=Mozilla/5.0 ... Chrome/114.0.0.0 Safari/537.36")
            self.driver = webdriver.Chrome(options=options)
            self.driver.implicitly_wait(5)
            self.driver.get(utils.URL)
            logger.debug("start initial login process.")
            auth.login(driver=self.driver, id=id, password=password)
        else:
            self.driver = driver
        
        # check opened page and logged in or not
        if self.driver.current_url.startswith(utils.URL):
            logger.debug("is in sbi page")
        else:
            logger.error("failed to open sbi page")
            raise Exception("failed to open sbi page")
        if auth.is_logged_in(self.driver):
            logger.debug("is logged in")
        else:
            logger.error("login is required.")
            raise Exception("login is required.")
        self.transit_to_screening_page()
        if self.switch_to_iframe():
            logger.debug("switched to iframe")
        else:
            logger.error("failed to switch to iframe")
            raise Exception("failed to switch to iframe")

    def switch_to_iframe(self):
        iframes = WebDriverWait(self.driver, 10).until(
            lambda d: d.find_elements(By.TAG_NAME, "iframe")
        )
        for iframe in iframes:
            self.driver.switch_to.frame(iframe)
            try:
                print("checking an iframe...")
                self.driver.find_element(By.CLASS_NAME, "WidgetTitle")
                print("Found PresetMenuBar in an iframe.")
                return True
            except exceptions.NoSuchElementException:
                self.driver.switch_to.default_content()
                continue
        return False
    
    def transit_to_screening_page(self):
        # transit to screening page
        try:
            target_class_name = "slc-global-nav-container"
            header_ele = self.driver.find_element(By.CLASS_NAME, target_class_name)
            lis = header_ele.find_element(By.XPATH, "ul").find_elements(By.XPATH, "li")
            lis[3].click()
            logger.debug("transit to stock page")
        except exceptions.NoSuchElementException as e:
            logger.error(f"failed to transit to stock page: {e}")
            raise e
        
        try:
            nav_headers = self.driver.find_elements(By.CLASS_NAME, "slc-nav-accordion-item")
            # open dialog
            for nav_header in nav_headers:
                if "銘柄検索" in nav_header.text:
                    nav_header.click()
                    break
            nav_items = nav_header.find_elements(By.XPATH, "ul/li")
            for nav_item in nav_items:
                if "銘柄スクリーニング" in nav_item.text:
                    # sometimes the element is not clickable, so scroll into view first
                    self.driver.execute_script("arguments[0].scrollIntoView({block: 'center'});", nav_item)
                    nav_item.click()
                    break
            logger.debug("transit to screening page")
        except exceptions.NoSuchElementException as e:
            logger.error(f"failed to transit to screening page: {e}")
            raise e
    
    def __get_basic_search_container(self, search_option: BASIC_SEARCH_CATEGORY):
        search_containers = self.driver.find_elements(By.CLASS_NAME, "Collapsible__contentInner")
        option_containers = search_containers[0].find_elements(By.CLASS_NAME, "innerCollapsible")
        container = option_containers[search_option.value]
        return container
        
    def __option_select(self, search_option: BASIC_SEARCH_CATEGORY, index: int, enable=True):
        container = self.__get_basic_search_container(search_option)
        checkboxe_parent = container.find_elements(By.CLASS_NAME, "SelectionBox")
        if index >= len(checkboxe_parent):
            raise ValueError("Invalid option value")
        target = checkboxe_parent[index].find_element(By.TAG_NAME, "input")
        checkbox = checkboxe_parent[index]
        if target.is_selected() is enable:
            return True
        checkbox.click()
        return target.is_selected() is enable

    def filter_market(self, market: MARKET_OPTIONS, enable=True):
        index = market.value if isinstance(market, MARKET_OPTIONS) else market
        return self.__option_select(BASIC_SEARCH_CATEGORY.MARKET, index, enable=enable)
    
    def filter_all_markets(self, enable=True):
        container = self.__get_basic_search_container(BASIC_SEARCH_CATEGORY.MARKET)
        checkboxe_parent = container.find_elements(By.CLASS_NAME, "SelectionBox")

        index = MARKET_OPTIONS.ALL.value
        if index >= len(checkboxe_parent):
            raise ValueError("Invalid option value")
        target = checkboxe_parent[index].find_element(By.TAG_NAME, "input")
        checkbox = checkboxe_parent[index]
        if enable:
            if target.is_selected():
                return True
            else:
                checkbox.click()
                return target.is_selected()
        else:
            if not target.is_selected():
                checkbox.click()
                if target.is_selected():
                    checkbox.click()
                    return target.is_selected()
                else:
                    return False
            else:
                checkbox.click()
                return not target.is_selected()

    def filter_size(self, size: SIZE_OPTIONS, enable=True):
        index = size.value if isinstance(size, SIZE_OPTIONS) else size
        return self.__option_select(BASIC_SEARCH_CATEGORY.SIZE, index, enable=enable)

    def filter_indicator(self, indicator: INDICATOR_OPTIONS, enable=True):
        index = indicator.value if isinstance(indicator, INDICATOR_OPTIONS) else indicator
        return self.__option_select(BASIC_SEARCH_CATEGORY.INDICATOR, index, enable=enable)

    def __open_sector_selection(self):
        try:
            container = self.__get_basic_search_container(BASIC_SEARCH_CATEGORY.SECTOR)
            container.find_element(By.CLASS_NAME, "selectionbox").click()
            return True
        except exceptions.ElementClickInterceptedException:
            # there is a popup dialog already
            dialogs = self.driver.find_elements(By.CLASS_NAME, "App-Popup")
            if len(dialogs) > 0:
                return True
        except exceptions.NoSuchElementException:
            print("No selectionbox found in the container to open sector selection.")
            return False
        return False
    
    def __close_to_cancel_sector(self):
        try:
            close_buttons = self.driver.find_elements(By.CLASS_NAME, "PopupTitleBar-Close")
            for button in close_buttons:
                if button.is_displayed() and button.is_enabled():
                    button.click()
                    return True
        except exceptions.NoSuchElementException:
            print("No close button found in the dialog.")
            return False
        return False

    def __close_to_apply_sector(self):
        try:
            apply_buttons = self.driver.find_elements(By.CLASS_NAME, "confirmbtn")
            for button in apply_buttons:
                if button.is_displayed() and button.is_enabled():
                    button.click()
                    return True
        except exceptions.NoSuchElementException:
            print("No apply button found in the dialog.")
            return False
        return False

    def filter_sector(self, sectors: list[int], enable=True):
        self.__open_sector_selection()
        sector_container = WebDriverWait(self.driver, 10).until(
            lambda d: d.find_element(By.CLASS_NAME, "SectorPopup")
        )
        sector_inputs = sector_container.find_elements(By.TAG_NAME, "input")
        sector_checkboxes = sector_container.find_elements(By.CLASS_NAME, "SelectionBox")

        if sector_inputs[-1].is_selected():
            sector_checkboxes[-1].click()
        else:
            sector_checkboxes[-1].click()
            sector_checkboxes[-1].click()
        for index in sectors:
            if index >= len(sector_checkboxes) - 1:
                raise ValueError("Invalid sector option value")
            target = sector_inputs[index]
            checkbox = sector_checkboxes[index]
            if target.is_selected() is enable:
                continue
            checkbox.click()
            if target.is_selected() is not enable:
                raise RuntimeError(f"Failed to set sector option at index {index}")
        self.__close_to_apply_sector()
        return True

    def filter_nisa(self, nisa: bool):
        enable = True if nisa else False
        return self.__option_select(BASIC_SEARCH_CATEGORY.NISA, 0, enable=enable)

    def filter_leverage(self, leverage: LEVERAGE_OPTIONS, enable=True):
        index = leverage.value if isinstance(leverage, LEVERAGE_OPTIONS) else leverage
        return self.__option_select(BASIC_SEARCH_CATEGORY.LEVERAGE, index, enable=enable)

    def filter_score(self, min_score: int):
        # not implemented yet
        raise NotImplementedError("filter_score is not implemented yet")

    def filter_trade_volume(self, min_trade_volume: int = None, max_trade_volume: int = None):
        volume_container = self.__get_basic_search_container(BASIC_SEARCH_CATEGORY.TRADE_VOLUME)
        input_elements = volume_container.find_elements(By.TAG_NAME, "input")
        if min_trade_volume is not None:
            input_elements[0].clear()
            input_elements[0].send_keys(str(min_trade_volume))
        if max_trade_volume is not None:
            input_elements[1].clear()
            input_elements[1].send_keys(str(max_trade_volume))
        return True
    
    def clear_trade_volume_filter(self):
        volume_container = self.__get_basic_search_container(BASIC_SEARCH_CATEGORY.TRADE_VOLUME)
        input_elements = volume_container.find_elements(By.TAG_NAME, "input")
        input_elements[0].clear()
        input_elements[1].clear()
        return True
    
    def select_item_from_dropdown(self, select_element, item_index):
        select = Select(select_element)
        # select.select_by_visible_text("3ヶ月前")
        # select.select_by_value("2")
        select.select_by_index(item_index)
    
    def __open_advanced_search_option_dialog(self):
        try:
            search_containers = self.driver.find_elements(By.CLASS_NAME, "Collapsible__contentInner")
            search_containers[1].find_element(By.CLASS_NAME, "addadvcriteria").click()
            WebDriverWait(self.driver, 10).until(EC.invisibility_of_element_located((By.CLASS_NAME, "App-Popup")))
            return True
        except exceptions.NoSuchElementException as e:
            adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
            if len(adv_search_condition_boxes) >= 10:
                print("Too many advanced search condition boxes found.")
                return False
            else:
                raise e
    
    def __select_menu(self, index: int):
        menu_element = self.driver.find_element(By.CLASS_NAME, "CriteriaMenuBar")
        menus = menu_element.find_elements(By.XPATH, "ul/li")
        if index >= len(menus):
            raise ValueError("Invalid menu index")
        menus[index].click()
        return True

    def __menu_number_of(self, index: int):
        menu_index = 0
        options_nums = [ADVANCED_FINANCIAL_OPTIONS_COUNT, ADVANCED_CONSENSUS_OPTIONS_COUNT, ADVANCED_PERFORMANCE_OPTIONS_COUNT, ADVANCED_TECHNICAL_OPTIONS_COUNT, ADVANCED_OTHERS_OPTIONS_COUNT]
        for i, num in enumerate(options_nums):
            if index < num:
                menu_index = i
                break
            index -= num
        return menu_index
    
    def __options_bulk_select_advanced(self, option_indices: list[int], enable=True):
        container = self.driver.find_element(By.CLASS_NAME, "AdvCriteriaPopup").find_element(By.CLASS_NAME, "fullmode")
        selection_boxes = container.find_elements(By.CLASS_NAME, "SelectionBox")
        for index in option_indices:
            if index >= len(selection_boxes):
                print(f"Invalid option index: {index}, skip it.")
                continue
            if not selection_boxes[index].is_displayed():
                menu_index = self.__menu_number_of(index)
                self.__select_menu(menu_index)

            input_element = selection_boxes[index].find_element(By.TAG_NAME, "input")
            if input_element.is_selected() is enable:
                continue
            else:
                self.driver.execute_script("arguments[0].click();", input_element)
                WebDriverWait(self.driver, 5).until(
                    lambda d: input_element.is_selected() == enable
                )

    def __option_select_advanced(self, index: int, enable=True):
        container = self.driver.find_element(By.CLASS_NAME, "AdvCriteriaPopup").find_element(By.CLASS_NAME, "fullmode")
        selection_boxes = container.find_elements(By.CLASS_NAME, "SelectionBox")
        if not selection_boxes[index].is_displayed():
            menu_index = self.__menu_number_of(index)
            self.__select_menu(menu_index)

        input_element = selection_boxes[index].find_element(By.TAG_NAME, "input")
        if input_element.is_selected() is enable:
            return True, False
        else:
            self.driver.execute_script("arguments[0].click();", input_element)
            WebDriverWait(self.driver, 5).until(
                lambda d: input_element.is_selected() == enable
            )
            return input_element.is_selected() is enable, True

    def __reset_advanced_options(self):
        total_option_num = ADVANCED_FINANCIAL_OPTIONS_COUNT + ADVANCED_CONSENSUS_OPTIONS_COUNT + ADVANCED_PERFORMANCE_OPTIONS_COUNT + ADVANCED_TECHNICAL_OPTIONS_COUNT + ADVANCED_OTHERS_OPTIONS_COUNT
        all_indices = list(range(total_option_num))
        bar_text = self.driver.find_element(By.CLASS_NAME, "PopupTitleBar-Name").text
        match = re.findall(r".* (\d+)/(\d+)", bar_text)
        if len(match) > 0:
            selected = int(match[0][0])
            remaining_to_deselect = selected
            # max_selectable = int(match[0][1])
            if selected > 0:
                for index in all_indices:
                    suc, changed = self.__option_select_advanced(index, False)
                    if changed:
                        remaining_to_deselect -= 1
                    if remaining_to_deselect <= 0:
                        print("All advanced options are reset.")
                        break
            else:
                print("No advanced options are selected, nothing to reset.")
        else:
            print("Failed to parse the selected and max selectable numbers from the title bar text.")
            self.__options_bulk_select_advanced(all_indices, False)

    def delete_all_advanced_search_conditions(self):
        adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
        for box in adv_search_condition_boxes:
            box.find_element(By.CLASS_NAME, "closebtn").click()
        remaining_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
        return len(remaining_boxes) == 0

    def __input_range_advance(self, title, min_value, max_value):
        adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
        for box in adv_search_condition_boxes:
            if title in box.text:
                input_elements = box.find_elements(By.TAG_NAME, "input")
                # index 0: enable, index 1: min, index 2: max
                input_elements[1].send_keys(Keys.CONTROL, "a")
                input_elements[1].send_keys(str(min_value))
                input_elements[2].send_keys(Keys.CONTROL, "a")
                input_elements[2].send_keys(str(max_value))
                self.driver.find_element("xpath", "//body").click()
                return True
        return False
    
    def __select_months(self, title, month_indices: list[int], enable=True):
        adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
        indices_of_months = [month - 1 for month in month_indices]
        for box in adv_search_condition_boxes:
            if title in box.text:
                month_container = box.find_element(By.CLASS_NAME, "MonthOption")
                month_checkboxes = month_container.find_elements(By.CLASS_NAME, "SelectionBox")
                month_inputs = month_container.find_elements(By.TAG_NAME, "input")
                if len(month_checkboxes) != 12 or len(month_inputs) != 12:
                    raise RuntimeError("Invalid month selection box structure.")
                for index in indices_of_months:
                    if index < 0 or index >= 12:
                        print(f"Invalid month index: {index}, skip it.")
                        continue
                    if month_inputs[index].is_selected() is enable:
                        continue
                    else:
                        self.driver.execute_script("arguments[0].click();", month_inputs[index])
                        WebDriverWait(self.driver, 5).until(
                            lambda d: month_inputs[index].is_selected() == enable
                        )
                return True
        return False

    def __reset_month_selection(self, title):
        return self.__select_months(title, list(range(1, 13)), False)
    
    def __delete_adv_filter(self, title):
        adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
        for box in adv_search_condition_boxes:
            if title in box.text:
                box.find_element(By.CLASS_NAME, "closebtn").click()
                return True
        return False
    
    def __set_advanced_search_option(self, option: ADVANCED_SEARCH_OPTION, variables: dict = None):
        if option.search_method == SEARCH_METHOD.RANGE:
            if variables is None or "min" not in variables or "max" not in variables:
                raise ValueError("min and max values are required for range option.")
            self.__input_range_advance(option.value, variables["min"], variables["max"])
        elif option.search_method == SEARCH_METHOD.DROPDOWN:
            if variables is None or "item_index" not in variables:
                raise ValueError("item_index is required for dropdown option.")
            adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
            for box in adv_search_condition_boxes:
                if option.value in box.text:
                    select_element = box.find_element(By.TAG_NAME, "select")
                    self.select_item_from_dropdown(select_element, variables["item_index"])
                    return True
            raise RuntimeError(f"Failed to find the advanced search condition box for {option.value}")
        elif option.search_method == SEARCH_METHOD.TWO_DROPDOWN:
            if variables is None or "first_item_index" not in variables or "second_item_index" not in variables:
                raise ValueError("first_item_index and second_item_index are required for two dropdown option.")
            adv_search_condition_boxes = self.driver.find_elements(By.CLASS_NAME, "CriteriaMiniBox")
            for box in adv_search_condition_boxes:
                if option.value in box.text:
                    select_elements = box.find_elements(By.TAG_NAME, "select")
                    if len(select_elements) != 2:
                        raise RuntimeError(f"Invalid number of select elements for {option.value}")
                    self.select_item_from_dropdown(select_elements[0], variables["first_item_index"])
                    self.select_item_from_dropdown(select_elements[1], variables["second_item_index"])
                    return True
            raise RuntimeError(f"Failed to find the advanced search condition box for {option.value}")
        elif option.search_method == SEARCH_METHOD.BOOLEAN:
            pass  # nothing to do, just need to be enabled
        elif option.search_method == SEARCH_METHOD.TABLE:
            if variables is None or "month_indices" not in variables:
                raise ValueError("month_indices is required for table type option.")
            month_indices = variables["month_indices"]
            if not isinstance(month_indices, list):
                raise ValueError("month_indices should be a list of integers.")
            self.__select_months(option.value, month_indices, True)
    
    def __convert_local_index_to_global_index(self, option: ADVANCED_SEARCH_OPTION):
        offset = 0
        if option.option_type != ADVANCED_SEARCH_CATEGORY.FINANCIAL:
            offset += ADVANCED_FINANCIAL_OPTIONS_COUNT
            if option.option_type != ADVANCED_SEARCH_CATEGORY.CONSENSUS:
                offset += ADVANCED_CONSENSUS_OPTIONS_COUNT
                if option.option_type != ADVANCED_SEARCH_CATEGORY.PERFORMANCE:
                    offset += ADVANCED_PERFORMANCE_OPTIONS_COUNT
                    if option.option_type != ADVANCED_SEARCH_CATEGORY.TECHNICAL:
                        offset += ADVANCED_TECHNICAL_OPTIONS_COUNT
        return offset + option.index
    
    def apply_advanced_filter(self, option: ADVANCED_SEARCH_OPTION, variables: dict = None):
        if self.__open_advanced_search_option_dialog():
            index = self.__convert_local_index_to_global_index(option)
            self.__option_select_advanced(index, True)
            # Set the filter values based on the option and variables
            self.__set_advanced_search_option(option, variables)
            # Click the apply button
            self.__close_to_apply_sector()
            return True
        return False

    def clear_all_filters(self):
        pass

    def __next_page(self):
        page_ul_element = self.driver.find_element(By.CLASS_NAME, "pagination")
        page_li_elements = page_ul_element.find_elements(By.TAG_NAME, "li")
        # Next link
        next_element = page_li_elements[-2].find_element(By.TAG_NAME, "a")
        if "disabled" in page_li_elements[-2].get_attribute("class"):
            print("No more pages.")
            return False
        else:
            next_element.click()
            # wait until loading spinner appears
            # if loading spinner appears, style = display: block;
            max_wait_count = 5  # seconds
            wait_count = 0
            while True and wait_count < max_wait_count:
                loading_element = self.driver.find_element(By.CLASS_NAME, "loading")
                style = loading_element.get_attribute("style")
                if "display: block;" in style:
                    break
                else:
                    time.sleep(0.5)
                    wait_count += 0.5
            # if loading spinner disappears, style = display: none;
            while True and wait_count < max_wait_count:
                try:
                    max_wait_count = 5  # seconds
                    wait_count = 0

                    loading_element = self.driver.find_element(By.CLASS_NAME, "loading")
                    style = loading_element.get_attribute("style")
                    if "display: none;" in style:
                        break
                    else:
                        time.sleep(0.5)
                        wait_count += 0.5
                except exceptions.NoSuchElementException:
                    break
        return True

    def get_search_results(self):
        table_element = self.driver.find_element(By.CLASS_NAME, "leftTable")
        rows = table_element.find_elements(By.TAG_NAME, "tr")
        results = []

        headers = ["security_code", "name", "market", "current_price", "price_change", "price_change_rate"]
        results.append(headers)
        while True:
            for row in rows[1:]:
                cols = row.find_elements(By.TAG_NAME, "td")
                last_col_text = cols[-1].text
                if last_col_text:
                    cols_text = [col.text for col in cols[:-1]]
                    current_price, price_change = last_col_text.split("\n")
                    re.search(r"([+-]?\d+\.?\d*)\s*\(([-+]?[\d\.]+%)\)", price_change)
                    match = re.search(r"([+-]?\d+\.?\d*)\s*\(([-+]?[\d\.]+%)\)", price_change)
                    if match:
                        price_change = match.group(1)
                        price_change_rate = match.group(2)
                    cols_text.append(current_price)
                    cols_text.append(price_change)
                    cols_text.append(price_change_rate)
                    results.append(cols_text)
                else:
                    break
            if not self.__next_page():
                break
            table_element = self.driver.find_element(By.CLASS_NAME, "leftTable")
            rows = table_element.find_elements(By.TAG_NAME, "tr")
        df_results = pd.DataFrame(results[1:], columns=results[0])
        df_results["security_code"] = df_results["security_code"]
        df_results["current_price"] = df_results["current_price"].str.replace(",", "").astype(float)
        df_results["price_change"] = df_results["price_change"].str.replace(",", "").astype(float)
        df_results["price_change_rate"] = df_results["price_change_rate"].str.replace("%", "").astype(float)
        return df_results