import PySimpleGUI as sg
import mplfinance as mpf
import inspect
import logging

from matplotlib.backends.backend_tkagg import FigureCanvasTkAgg
import finance_research.stockDF as stockDF
class StockGUI:
    def __init__(self, stock_db, ticker_list: list, ticker_names: list, stock_data: list):
        self.ticker_list = ticker_list
        self.ticker_names = ticker_names
        
        self.stock_db = stock_db
        self.stock_df = None
        
        self.w_width = 1000
        self.w_height = 650
        
        self.stock_data = stock_data
        
        self.canvas_packed = {}
        
    def set_stock_df(self, stock_df):
        self.stock_df = stock_df
        
    def integrate_ticker_and_names(self):
        ticker_and_names = []
        for ticker, names in zip(self.ticker_list, self.ticker_names):
            ticker_and_names.append(ticker + ' ' + names)
        
        return ticker_and_names
        
    def run(self):
        sg.theme('LightGray2')
        self.ticker_list_box = [
            [sg.Listbox(values=self.integrate_ticker_and_names(), enable_events=True, 
                        size=(30, 30), key='TICKER_LIST')]
        ]
        self.duration_tab_group = [[
            sg.Tab('YEAR', [[sg.Canvas(size=(550, 340), key='CANVAS_YEAR')]]),
            sg.Tab('MONTH', [[sg.Canvas(size=(550, 340), key='CANVAS_MONTH')]]),
            sg.Tab('WEEK', [[sg.Canvas(size=(550, 340), key='CANVAS_WEEK')]]),
        ]]
        self.layout = [
            [sg.Text('STOCK GUI TEST', font=('current 18'))],
            [sg.HSep()],
            [sg.Col(self.ticker_list_box), sg.TabGroup(self.duration_tab_group, enable_events=True)],
            [sg.HSep()],
        ]
        
        window = sg.Window('finance_research', self.layout, size=(self.w_width, self.w_height), grab_anywhere=True, finalize=True)
        fig_agg = None
        selected_duration = None
        
        while True:
            event, values = window.read()
            choice = values['TICKER_LIST'][0].split(' ')[0]
            
            ticker_idx = self.ticker_list.index(choice)
            stock_data = self.stock_data[ticker_idx]
            
            if event in (sg.WIN_CLOSED, 'Exit'):
                break
            if fig_agg:
                self.delete_figure_agg(fig_agg)
                
            if values[event] in ('WEEK', 'MONTH', 'YEAR'):
                selected_duration = values[event]
                
            try:
                if selected_duration: 
                    stock_df = stockDF.StockDF(stock_data)
                    self.set_stock_df(stock_df)
                    fig = self.plot_chart(selected_duration)
                    
                fig_agg = self.plot_figure(window['CANVAS_'+selected_duration].TKCanvas, fig)
            except Exception as e:
                logging.warning(f'Exception in function: {e}')
            
        window.close() 
        

    def plot_chart(self, duration: str):
        def plot_duration(duration, style):
            if duration == 'YEAR':
                return mpf.plot(self.stock_df.stock_year(), figsize=(2.8, 1.65), style=style, type='line', volume=True, returnfig=True)
            elif duration == 'MONTH':
                return mpf.plot(self.stock_df.stock_month(), figsize=(2.8, 1.65), type='candle', style=style, volume=True, returnfig=True)
            elif duration == 'WEEK':
                return mpf.plot(self.stock_df.stock_week(), figsize=(2.8, 1.65), type='candle', style=style, volume=True, returnfig=True)
            elif duration == 'DAY':
                return mpf.plot(self.stock_df.stock_day(), figsize=(2.8, 1.65), type='candle', style=style, volume=True, returnfig=True)
        
        style = mpf.make_mpf_style(base_mpf_style='yahoo', rc={'font.size':3})
        fig, _ = plot_duration(duration, style)
        
        return fig
    
    
    def plot_figure(self, canvas, fig):
        fig_canvas_agg = FigureCanvasTkAgg(fig, master=canvas)
        fig_canvas_agg.draw()
        fig_canvas_agg.get_tk_widget().pack(side='right', fill='both', expand=1)
        
        widget = fig_canvas_agg.get_tk_widget()
        if widget not in self.canvas_packed:
            self.canvas_packed[widget] = fig
            widget.pack(side='top', fill='both', expand=1)
            
        return fig_canvas_agg
        
            
    def delete_figure_agg(self, fig_agg):
        fig_agg.get_tk_widget().forget()
        try:
            self.canvas_packed.pop(fig_agg.get_tk_widget())
        except Exception as e:
            print(f'Error removing {fig_agg} from list', e)